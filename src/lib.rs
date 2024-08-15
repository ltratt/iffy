#![allow(clippy::type_complexity)]

use filetime::{set_file_times, FileTime};
use flate2::{Compression, GzBuilder};
use ignore::WalkBuilder;
use once_cell::sync::Lazy;
use pulldown_cmark::{CowStr, Event, Options, Parser, Tag, TagEnd};
use regex::{Regex, RegexBuilder};
use std::{
    collections::HashMap,
    error::Error,
    fs::{self, create_dir_all, read_to_string, File},
    io::Write,
    path::{Path, PathBuf},
    process,
    sync::{Arc, Mutex},
};
use tera::{self, Context, Tera};
use url::Url;
use wyhash::wyhash;

mod highlight;
use highlight::Highlighter;

const CSS_EXTENSION: &str = "css";
const MARKDOWN_EXTENSION: &str = "md";
const HTML_EXTENSION: &str = "html";
const TERAFY_EXTENSIONS: &[&str] = &["rss"];

static RE_TOML: Lazy<Regex> = Lazy::new(|| {
    RegexBuilder::new(r"\A\+\+\+\s*?$(.*?)^\+\+\+\s*$")
        .dot_matches_new_line(true)
        .multi_line(true)
        .build()
        .unwrap()
});

pub enum FootnoteStyle {
    Plain,
    Popup,
}

pub struct Iffy {
    host: Url,
    verbose: bool,
    deploy: bool,
    absolute_urls: bool,
    footnote_style: FootnoteStyle,
    src_dir: PathBuf,
    output_dir: PathBuf,
    tera: Arc<Mutex<Tera>>,
    highlighter: Highlighter,
    file_filter: Option<Box<dyn Fn(&Path) -> bool + Send + Sync>>,
    navcrumb_cache: Arc<Mutex<HashMap<String, String>>>,
    navcrumb_filter: Option<Box<dyn Fn(Vec<String>) -> Vec<String> + Send + Sync>>,
    toml_processor: Option<
        Box<dyn Fn(&Path, toml::Table) -> Result<toml::Table, Box<dyn Error>> + Send + Sync>,
    >,
    footnote_seed: u64,
}

impl Iffy {
    pub fn new(
        host: Url,
        src_dir: PathBuf,
        output_dir: PathBuf,
        templates_dir: PathBuf,
        syntect_theme: &str,
        footnote_seed: u64,
    ) -> Self {
        fn strip_trailing_slash(p: PathBuf) -> PathBuf {
            PathBuf::from(p.as_path().to_str().unwrap().trim_end_matches('/'))
        }
        Self {
            host,
            verbose: false,
            deploy: false,
            absolute_urls: false,
            footnote_style: FootnoteStyle::Popup,
            output_dir: strip_trailing_slash(output_dir),
            src_dir: strip_trailing_slash(src_dir),
            tera: Arc::new(Mutex::new(tera_err(Tera::new(&format!(
                "{}/**/*.html",
                templates_dir.as_path().to_str().unwrap()
            ))))),
            highlighter: Highlighter::new(syntect_theme).unwrap(),
            file_filter: None,
            navcrumb_cache: Arc::new(Mutex::new(HashMap::new())),
            navcrumb_filter: None,
            toml_processor: None,
            footnote_seed,
        }
    }

    /// Print out which file is being processed at any given point? Note this also makes Iffy use a
    /// single thread (so that the verbose output can be correlated with what's actually running).
    pub fn verbose(&mut self, yes: bool) {
        self.verbose = yes;
    }

    /// Turn on settings for a full deploy (e.g. minimise CSS, generate .gz files). This will slow
    /// down processing considerably.
    pub fn deploy(&mut self) {
        self.deploy = true;
    }

    /// Convert relative URLs in Markdown to absolute URLs? Not generally recommended for sites
    /// delivered by http, but may be useful for pages delivered by other means. Defaults to
    /// `false`.
    pub fn absolute_urls(&mut self) {
        self.absolute_urls = true;
    }

    /// Set the [FootnoteStyle].
    pub fn footnote_style(&mut self, fns: FootnoteStyle) {
        self.footnote_style = fns;
    }

    /// For each path found in [self.src_dir], call `f`, and exclude the path from consideration if
    /// `f` returns false.
    pub fn file_filter<F>(&mut self, f: F)
    where
        F: Fn(&Path) -> bool + Send + Sync + 'static,
    {
        self.file_filter = Some(Box::new(f));
    }

    pub fn navcrumb_filter<F>(&mut self, f: F)
    where
        F: Fn(Vec<String>) -> Vec<String> + Send + Sync + 'static,
    {
        self.navcrumb_filter = Some(Box::new(f));
    }

    /// Pass TOML data at the start of a markdown file to `f` for preprocessing. `f` must return
    /// [toml::Table] data which will then be used as if it was what written in the markdown file.
    pub fn toml_processor<F>(&mut self, f: F)
    where
        F: Fn(&Path, toml::Table) -> Result<toml::Table, Box<dyn Error>> + Send + Sync + 'static,
    {
        self.toml_processor = Some(Box::new(f));
    }

    pub fn register_tera_function<F, T>(&mut self, name: &str, f: F, context: T)
    where
        F: Fn(&HashMap<String, tera::Value>, &T) -> tera::Result<tera::Value>
            + Send
            + Sync
            + 'static,
        T: Send + Sync + 'static,
    {
        self.tera
            .lock()
            .unwrap()
            .register_function(name, TeraFunction { f, context });
    }

    pub fn build(self) {
        let sa = Arc::new(self);
        // A very simple hack to push processing into multiple threads.
        let tp = rpools::pool::WorkerPool::new(num_cpus::get());
        let wg = rpools::sync::WaitGroup::default();
        for e in WalkBuilder::new(&sa.src_dir)
            .build()
            .map(|e| e.unwrap())
            .filter(|e| e.path().is_file())
        {
            if let Some(ff) = &sa.file_filter {
                if !ff(e.path()) {
                    continue;
                }
            }

            let cl = {
                let sa = Arc::clone(&sa);
                let wg = wg.clone();
                move || {
                    if let Err(err) = sa.process(e.path()) {
                        eprintln!(
                            "Error while processing {}:\n\n{}",
                            e.path().as_os_str().to_str().unwrap(),
                            err
                        );
                        process::exit(1);
                    }
                    drop(wg);
                }
            };

            if sa.verbose {
                cl();
            } else {
                tp.execute(cl);
            }
        }

        if !sa.verbose {
            wg.wait();
        }
    }

    fn process(&self, inp: &Path) -> Result<(), Box<dyn Error>> {
        if self.verbose {
            println!("{}", inp.to_str().unwrap());
        }
        if let Some(x) = inp.extension().map(|x| x.to_str().unwrap()) {
            let outp = if x == MARKDOWN_EXTENSION {
                self.process_md(inp)?
            } else if x == HTML_EXTENSION {
                self.process_html(inp)?
            } else if x == CSS_EXTENSION {
                Some(self.process_css(inp)?)
            } else if TERAFY_EXTENSIONS.contains(&x) {
                let d = read_to_string(inp)?;
                let outp = self.site_to_out(inp);
                fs::write(
                    &outp,
                    tera_err(self.tera.lock().unwrap().render_str(&d, &Context::new())),
                )?;
                Some(outp)
            } else {
                None
            };
            if let Some(outp) = outp {
                if self.deploy {
                    let mut gzipp = outp.clone();
                    gzipp.set_extension(format!(
                        "{}.gz",
                        outp.extension().unwrap().to_str().unwrap()
                    ));
                    let f = File::create(gzipp)?;
                    let mut gz = GzBuilder::new()
                        .filename(outp.file_name().unwrap().to_str().unwrap())
                        .write(f, Compression::best());
                    gz.write_all(read_to_string(outp)?.as_bytes())?;
                    gz.finish()?;
                }
                return Ok(());
            }
        }

        let inp_metadata = fs::metadata(inp)?;
        let outp = self.site_to_out(inp);
        let copy = if outp.exists() {
            match (
                inp_metadata.modified(),
                fs::metadata(&outp).map(|x| x.modified()),
            ) {
                (Ok(l), Ok(Ok(r))) => l != r,
                _ => todo!(),
            }
        } else {
            true
        };
        if copy {
            fs::copy(inp, &outp)?;
            set_file_times(
                outp,
                FileTime::from_system_time(inp_metadata.accessed()?),
                FileTime::from_system_time(inp_metadata.modified()?),
            )?;
        }
        Ok(())
    }

    fn process_css(&self, inp: &Path) -> Result<PathBuf, Box<dyn Error>> {
        let outp = self.site_to_out(inp);
        let d = read_to_string(inp)?;
        let rawcss = tera_err(self.tera.lock().unwrap().render_str(&d, &Context::new()));
        let css = if self.deploy {
            let mut m = css_minify::optimizations::Minifier::default();
            m.minify(&rawcss, css_minify::optimizations::Level::One)
                .map_err(|x| x.to_string())?
        } else {
            rawcss
        };
        fs::write(&outp, css)?;
        Ok(outp)
    }

    fn process_md(&self, inp: &Path) -> Result<Option<PathBuf>, Box<dyn Error>> {
        let d = read_to_string(inp)?;
        let bodytxt;
        let mut toml;
        if let Some(cs) = RE_TOML.captures(&d) {
            toml = cs.get(1).unwrap().as_str().parse::<toml::Table>()?;
            bodytxt = &d[cs.get(0).unwrap().len()..];
        } else {
            toml = toml::Table::new();
            bodytxt = &d;
        }
        if let Some(tpp) = &self.toml_processor {
            toml = tpp(inp, toml)?;
        }
        if self.deploy && toml.get("deploy").map(|x| x.as_bool()) == Some(Some(false)) {
            return Ok(None);
        }

        let mut ctx = Context::new();
        for (k, v) in &toml {
            match v {
                toml::Value::String(s) => {
                    ctx.insert(k, &s);
                }
                toml::Value::Boolean(b) => {
                    ctx.insert(k, b);
                }
                toml::Value::Array(x) => {
                    ctx.insert(k, x);
                }
                _ => todo!(),
            }
        }

        let bodytxt = tera_err(self.tera.lock().unwrap().render_str(bodytxt, &ctx));

        let mut options = Options::empty();
        options.insert(Options::ENABLE_FOOTNOTES);
        options.insert(Options::ENABLE_SMART_PUNCTUATION);
        options.insert(Options::ENABLE_TABLES);
        let ev = Parser::new_ext(&bodytxt, options);
        let ev = if self.absolute_urls {
            absolute_urls(ev.into_iter(), &self.host).collect::<Vec<_>>()
        } else {
            ev.into_iter().collect::<Vec<_>>()
        };
        let ev = titles_to_links(ev);
        let ev = img_to_video(ev.into_iter());
        let ev = self.highlighter.highlight(ev.into_iter())?;
        // Footnote extraction must come last as it extracts data (i.e. if it comes earlier in the
        // chain, things like syntax highlighting won't apply to footnotes).
        let (ev, fnotes) =
            self.process_footnotes(ev.into_iter(), inp.as_os_str().to_str().unwrap())?;
        let mut md = String::new();
        pulldown_cmark::html::push_html(&mut md, ev.into_iter());

        let mut rendered_fnotes = Vec::with_capacity(2 * fnotes.len());
        for (i, (name, ev)) in fnotes.into_iter().enumerate() {
            let hash = hash_link(
                self.footnote_seed,
                inp.as_os_str().to_str().unwrap(),
                &name.to_string(),
            );
            let off = i + 1;
            let mut fnote_body = String::new();
            pulldown_cmark::html::push_html(&mut fnote_body, ev.into_iter());

            match self.footnote_style {
                FootnoteStyle::Plain => {
                    rendered_fnotes.push(format!(r#"<div class="footnote_def"><a name="{hash}"><span style="padding-right: 1ch">[{off}]</span></a><div class="footnote_def_body">{fnote_body}</div></div>"#));
                }
                FootnoteStyle::Popup => {
                    rendered_fnotes.push(format!(r#"<div class="footnote_def"><a name="{hash}"><span style="padding-right: 1ch">[{off}]</span></a><div class="footnote_def_body">{fnote_body}</div></div>"#));
                    rendered_fnotes.push(format!(r#"<div id="pu_{hash}" class="footnote_pu" onclick="this.style.display='none'"><div class="footnote_close"><span class="footnote_close_icon">☒</span></div>{fnote_body}</div>"#));
                }
            }
        }

        let mut outp = self.site_to_out(inp);
        outp.set_extension(HTML_EXTENSION);

        ctx.insert("body", &md);
        ctx.insert("footnotes", &rendered_fnotes.join("\n"));
        ctx.insert("navcrumbs", &self.navcrumbs(inp)?);
        ctx.insert(
            "out_path",
            outp.strip_prefix(&self.output_dir)
                .unwrap()
                .to_str()
                .unwrap(),
        );

        let template = match toml.get("template") {
            Some(toml::Value::String(x)) => x,
            Some(_) => todo!(),
            _ => "base.html",
        };
        tera_err(
            self.tera
                .lock()
                .unwrap()
                .render_to(template, &ctx, File::create(&outp)?),
        );
        Ok(Some(outp))
    }

    fn process_html(&self, inp: &Path) -> Result<Option<PathBuf>, Box<dyn Error>> {
        let d = read_to_string(inp)?;
        let bodytxt;
        let mut toml;
        let outp = self.site_to_out(inp);
        // Only process HTML files if they start with a TOML section `+++\n...\n+++`.
        if let Some(cs) = RE_TOML.captures(&d) {
            toml = cs.get(1).unwrap().as_str().parse::<toml::Table>()?;
            bodytxt = &d[cs.get(0).unwrap().len()..];
        } else {
            fs::write(&outp, d)?;
            return Ok(Some(outp));
        }
        if let Some(tpp) = &self.toml_processor {
            toml = tpp(inp, toml)?;
        }
        if self.deploy && toml.get("deploy").map(|x| x.as_bool()) == Some(Some(false)) {
            return Ok(None);
        }

        let mut ctx = Context::new();

        for (k, v) in &toml {
            match v {
                toml::Value::String(s) => {
                    ctx.insert(k, &s);
                }
                toml::Value::Boolean(b) => {
                    ctx.insert(k, b);
                }
                _ => todo!(),
            }
        }

        ctx.insert("body", &bodytxt);
        ctx.insert("navcrumbs", &self.navcrumbs(inp)?);
        ctx.insert(
            "out_path",
            outp.strip_prefix(&self.output_dir)
                .unwrap()
                .to_str()
                .unwrap(),
        );

        let template = match toml.get("template") {
            Some(toml::Value::String(x)) => x,
            Some(_) => todo!(),
            _ => "base.html",
        };
        tera_err(
            self.tera
                .lock()
                .unwrap()
                .render_to(template, &ctx, File::create(&outp)?),
        );
        Ok(Some(outp))
    }

    fn navcrumbs(&self, inp: &Path) -> Result<tera::Value, Box<dyn Error>> {
        assert!(inp.is_file());
        let mut curp = inp.parent().unwrap().to_owned();
        if inp.file_name().unwrap() == "index.md" && curp != self.src_dir {
            curp = curp.parent().unwrap().to_owned();
        }
        let mut nc = Vec::new();
        while curp.starts_with(&self.src_dir) {
            assert!(curp.is_dir());
            curp.push("index.md");
            let mut nclk = self.navcrumb_cache.lock().unwrap();
            if nclk.get(curp.as_os_str().to_str().unwrap()).is_none() && curp.is_file() {
                let d = read_to_string(&curp).unwrap();
                let toml = if let Some(cs) = RE_TOML.captures(&d) {
                    cs.get(1).unwrap().as_str().parse::<toml::Table>()?
                } else {
                    toml::Table::new()
                };
                if let Some(Some(title)) = toml.get("title").map(|x| x.as_str()) {
                    nclk.insert(
                        curp.as_os_str().to_str().unwrap().to_owned(),
                        title.to_owned(),
                    );
                }
            }
            if let Some(title) = nclk.get(curp.as_os_str().to_str().unwrap()) {
                nc.push(format!(
                    r#"<a href="{}/">{title}</a>"#,
                    curp.parent()
                        .unwrap()
                        .as_os_str()
                        .to_str()
                        .unwrap()
                        .strip_prefix(self.src_dir.to_str().unwrap())
                        .unwrap()
                ));
            }
            curp = curp.parent().unwrap().parent().unwrap().to_owned();
        }
        nc.reverse();
        if let Some(nf) = &self.navcrumb_filter {
            nc = nf(nc);
        }
        Ok(tera::Value::Array(
            nc.into_iter().map(tera::Value::String).collect::<Vec<_>>(),
        ))
    }

    /// Convert a path `site/a/b/c.d` to `out/a/b/c.d`. Ensures that the directory structure in
    /// `out` is sufficient to write to that output path.
    pub fn site_to_out(&self, p: &Path) -> PathBuf {
        let mut out_path = PathBuf::new();
        out_path.push(&self.output_dir);
        out_path.push(p.strip_prefix(&self.src_dir).unwrap());
        create_dir_all(out_path.as_path().parent().unwrap()).unwrap();
        out_path
    }

    fn process_footnotes<'a, It>(
        &self,
        ev_in: It,
        inp: &str,
    ) -> Result<(Vec<Event<'a>>, Vec<(String, Vec<Event<'a>>)>), Box<dyn Error>>
    where
        It: Iterator<Item = Event<'a>>,
    {
        let mut ev_out = Vec::new();
        let mut fnotes = Vec::new();
        let mut fnotes_seen = HashMap::new();
        let mut fnote = None;
        for e in ev_in {
            match e {
                Event::Start(Tag::FootnoteDefinition(name)) => {
                    assert!(fnote.is_none());
                    fnote = Some((name.to_string(), Vec::new()));
                }
                Event::End(TagEnd::FootnoteDefinition) => {
                    fnotes.push(fnote.unwrap());
                    fnote = None;
                }
                Event::FootnoteReference(name) => {
                    let name = name.to_string();
                    if fnotes_seen.contains_key(&name) {
                        return Err(format!("Multiple use of footnote '{}'", name).into());
                    }
                    fnotes_seen.insert(name.clone(), fnotes_seen.len());
                    let hash = hash_link(self.footnote_seed, inp, &name);
                    match self.footnote_style {
                        FootnoteStyle::Plain => {
                            ev_out.push(Event::Html(format!("[{}]", fnotes_seen.len()).into()));
                        }
                        FootnoteStyle::Popup => {
                            ev_out.push(Event::Html(
                    format!(r#"<span class="footnote_ref"><a href="javascript:;" onclick="e=document.getElementById('pu_{hash}');e.style.top=this.offsetTop+'px';e.style.display='block';">[{}]</a></span>"#, fnotes_seen.len()).into()
                ));
                        }
                    }
                }
                x => match fnote {
                    Some((_, ref mut fnote)) => fnote.push(x),
                    None => ev_out.push(x),
                },
            }
        }
        assert!(fnote.is_none());
        for k in fnotes_seen.keys() {
            if !fnotes.iter().any(|x| &x.0 == k) {
                return Err(format!("Footnote '{k}' has no matching definition").into());
            }
        }
        for (k, _) in fnotes.iter() {
            if !fnotes_seen.contains_key(k) {
                return Err(format!("Footnote '{k}' is not referenced in the text").into());
            }
        }
        fnotes.sort_unstable_by(|a, b| fnotes_seen[&a.0].partial_cmp(&fnotes_seen[&b.0]).unwrap());

        Ok((ev_out, fnotes))
    }
}

struct TeraFunction<
    F: Fn(&HashMap<String, tera::Value>, &T) -> tera::Result<tera::Value> + Send + Sync + 'static,
    T,
> {
    f: F,
    context: T,
}

impl<
        F: Fn(&HashMap<String, tera::Value>, &T) -> tera::Result<tera::Value> + Send + Sync + 'static,
        T: Send + Sync,
    > tera::Function for TeraFunction<F, T>
{
    fn call(&self, args: &HashMap<String, tera::Value>) -> tera::Result<tera::Value> {
        (self.f)(args, &self.context)
    }

    fn is_safe(&self) -> bool {
        true
    }
}

/// Rewrite `<h1>Blah1 Blah2!</h2>` to `<h1><a name="blah1_blah2">Blah1 Blah2!</a></h1>` (and for
/// `H2`, `H3` etc.)
pub fn titles_to_links<'a, It>(ev_in: It) -> impl Iterator<Item = Event<'a>>
where
    It: IntoIterator<Item = Event<'a>>,
{
    let mut ev_in = ev_in.into_iter();
    let mut ev_out = Vec::new();
    let mut next = ev_in.next();
    while let Some(e) = next {
        match e {
            Event::Start(Tag::Heading { .. }) => {
                ev_out.push(e);
                next = ev_in.next();
                let mut title_txt = Vec::new();
                let mut title_ev = Vec::new();
                loop {
                    match next {
                        Some(Event::End(TagEnd::Heading(_))) => break,
                        Some(Event::Text(ref s)) => {
                            title_txt.push(s.to_owned());
                            title_ev.push(Event::Text(s.clone()));
                        }
                        Some(x) => title_ev.push(x),
                        None => panic!(),
                    }
                    next = ev_in.next();
                }
                let link_name = title_txt
                    .iter()
                    .map(|x| {
                        x.to_lowercase()
                            .replace(' ', "_")
                            .chars()
                            .filter(|x| *x == '_' || x.is_alphanumeric())
                            .collect::<String>()
                    })
                    .collect::<Vec<_>>()
                    .join("_");
                ev_out.push(Event::Html(format!(r#"<a name="{link_name}">"#).into()));
                ev_out.extend_from_slice(title_ev.as_slice());
                ev_out.push(Event::Html("</a>".into()));
            }
            _ => {
                ev_out.push(e);
                next = ev_in.next();
            }
        }
    }
    ev_out.into_iter()
}

/// Rewrite `<img src="x.mp4">` to `<video><source src="x.mp4"></video>`.
pub fn img_to_video<'a, It>(ev_in: It) -> impl Iterator<Item = Event<'a>>
where
    It: Iterator<Item = Event<'a>>,
{
    ev_in.into_iter().map(|e| match e {
        Event::Start(Tag::Image {
            link_type,
            dest_url,
            title,
            id,
        }) => {
            if dest_url.as_ref().ends_with(".mp4") {
                Event::Html(CowStr::from(format!(
                    r#"<video controls><source src="{}" type="video/mp4" /><a href="{}">[Video]</a></video>"#,
                    dest_url.as_ref(), dest_url.as_ref()
                )))
            } else {
                Event::Start(Tag::Image {
                    link_type,
                    dest_url,
                    title,
                    id,
                })
            }
        }
        x => x,
    })
}

/// Convert relative URLs to absolute URLs.
pub fn absolute_urls<'a, 'b, It>(ev_in: It, host: &'b Url) -> impl Iterator<Item = Event<'a>> + '_
where
    It: Iterator<Item = Event<'a>> + 'b,
{
    ev_in.into_iter().map(|e| match e {
        Event::Start(Tag::Link {
            link_type,
            dest_url,
            title,
            id,
        }) => Event::Start(Tag::Link {
            link_type,
            dest_url: host.join(dest_url.as_ref()).unwrap().to_string().into(),
            title,
            id,
        }),
        Event::Start(Tag::Image {
            link_type,
            dest_url,
            title,
            id,
        }) => Event::Start(Tag::Image {
            link_type,
            dest_url: host.join(dest_url.as_ref()).unwrap().to_string().into(),
            title,
            id,
        }),
        x => x,
    })
}

fn tera_err<T>(x: Result<T, tera::Error>) -> T {
    match x {
        Ok(x) => x,
        Err(e) => {
            if let Some(s) = e.source() {
                eprintln!("{e}\n{s}");
            } else {
                eprintln!("{e}");
            }
            process::exit(1);
        }
    }
}

fn hash_link(seed: u64, path: &str, link: &str) -> String {
    // wyhash is stable and predictable.
    let mut h = format!("{:x}", wyhash(format!("{path}_{link}").as_bytes(), seed));
    h.truncate(12);
    h
}
