use anyhow::anyhow;

use crate::token::Token;

use std::str::Chars;

pub struct Cursor<'a> {
    chars: Chars<'a>,
}

const EOF: char = '\0';

impl<'a> Cursor<'a> {
    pub fn new(source: &'a str) -> Self {
        Self {
            chars: source.chars(),
        }
    }

    pub fn advance_token(&mut self) -> anyhow::Result<Token> {
        let token = match self.bump() {
            '(' => Token::LeftParen,
            ')' => Token::RightParen,
            '[' => Token::LeftBracket,
            ']' => Token::RightBracket,
            ';' => Token::Semicolon,
            '*' => Token::Star,
            '=' => Token::Equal,
            ' ' | ',' | '\r' | '\t' | '\n' => self.advance_token()?,
            c if c == '"' || c == '\'' => self.string(c)?,
            c @ '0'..='9' => self.number(c)?,
            c if c.is_alphanumeric() => self.identifier(c),
            EOF => Token::Eof,
            a => return Err(anyhow!("found invalid character {a:?}")),
        };

        Ok(token)
    }

    fn identifier(&mut self, first_char: char) -> Token {
        let mut ident = String::from(first_char);

        while matches!(self.first(), c if c != ' ' && c != ',' && c != ';' && c != ')' && c != EOF)
        {
            ident.push(self.bump());
        }

        if let Some(token) = self.keyword(&ident) {
            return token;
        }

        Token::Identifier(ident)
    }

    fn number(&mut self, first_char: char) -> anyhow::Result<Token> {
        let mut number = String::from(first_char);

        while let '0'..='9' = self.first() {
            number.push(self.bump());
        }
        if self.first() == '.' {
            number.push(self.bump());
            while let '0'..='9' = self.first() {
                number.push(self.bump());
            }
        }

        let number = number.parse::<f64>()?;
        Ok(Token::Number(number))
    }

    fn string(&mut self, delimiter: char) -> anyhow::Result<Token> {
        let mut string = String::new();

        while matches!(self.first(), c if c != delimiter) {
            string.push(self.bump());
        }

        if self.first() != delimiter {
            return Err(anyhow!("string untermineted"));
        }

        self.bump();
        Ok(Token::Str(string))
    }

    fn keyword(&self, ident: &str) -> Option<Token> {
        match &*ident.to_lowercase() {
            "true" => Some(Token::True),
            "false" => Some(Token::False),
            "select" => Some(Token::Select),
            "insert" => Some(Token::Insert),
            "into" => Some(Token::Into),
            "values" => Some(Token::Values),
            "update" => Some(Token::Update),
            "set" => Some(Token::Set),
            "delete" => Some(Token::Delete),
            "from" => Some(Token::From),
            "where" => Some(Token::Where),
            "zone" => Some(Token::Zone),
            _ => None,
        }
    }

    fn bump(&mut self) -> char {
        self.chars.next().unwrap_or(EOF)
    }

    fn first(&self) -> char {
        self.chars.clone().next().unwrap_or(EOF)
    }
}
