use pulldown_cmark::{CodeBlockKind, CowStr, Event, Tag, TagEnd};
use std::error::Error;
use syntect::{
    highlighting::{Theme, ThemeSet},
    html::highlighted_html_for_string,
    parsing::SyntaxSet,
};

pub struct Highlighter {
    syntaxset: SyntaxSet,
    theme: Theme,
}

impl Highlighter {
    pub fn new(theme: &str) -> Result<Self, Box<dyn Error>> {
        Ok(Highlighter {
            syntaxset: SyntaxSet::load_defaults_newlines(),
            theme: ThemeSet::load_defaults().themes[theme].clone(),
        })
    }

    pub fn highlight<'a, It>(&self, ev_in: It) -> Result<Vec<Event<'a>>, Box<dyn Error>>
    where
        It: Iterator<Item = Event<'a>>,
    {
        let mut ev_out = Vec::new();
        let mut syn = self.syntaxset.find_syntax_plain_text();
        let mut cb = None;

        for event in ev_in {
            match event {
                Event::Start(Tag::CodeBlock(kind)) => {
                    assert!(cb.is_none());
                    cb = Some(Vec::new());
                    syn = match kind {
                        CodeBlockKind::Fenced(lang) => self
                            .syntaxset
                            .find_syntax_by_token(&lang)
                            .unwrap_or_else(|| self.syntaxset.find_syntax_plain_text()),
                        CodeBlockKind::Indented => self.syntaxset.find_syntax_plain_text(),
                    }
                }
                Event::End(TagEnd::CodeBlock) => {
                    let html = highlighted_html_for_string(
                        cb.take().unwrap().join("").as_str(),
                        &self.syntaxset,
                        syn,
                        &self.theme,
                    )?;
                    ev_out.push(Event::Html(CowStr::from(html)));
                }
                Event::Text(s) => match cb {
                    Some(ref mut cb) => cb.push(s),
                    None => ev_out.push(Event::Text(s)),
                },
                x => {
                    if cb.is_some() {
                        todo!();
                    }
                    ev_out.push(x);
                }
            }
        }

        Ok(ev_out)
    }
}
