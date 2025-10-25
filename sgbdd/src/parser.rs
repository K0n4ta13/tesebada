use anyhow::anyhow;

use super::Token;

#[derive(Debug, Clone)]
pub enum Query {
    Select {
        table: String,
        fields: Vec<String>,
        filter: Option<Where>,
        zones: Option<Vec<String>>,
    },
    Insert {
        table: String,
        columns: Vec<String>,
        values: Vec<Vec<String>>,
    },
    Update {
        table: String,
        assignments: Vec<(String, String)>,
        filter: Option<Where>,
        zones: Option<Vec<String>>,
    },
    Delete {
        table: String,
        filter: Option<Where>,
        zones: Option<Vec<String>>,
    },
}

#[derive(Debug, Clone)]
pub struct Where {
    pub column: String,
    pub op: String,
    pub value: String,
}

pub struct Parser {
    tokens: Vec<Token>,
}

impl Parser {
    pub fn new(mut tokens: Vec<Token>) -> Parser {
        tokens.reverse();
        Parser { tokens }
    }

    pub fn parse(&mut self) -> anyhow::Result<Query> {
        match self.bump()? {
            Token::Select => self.parse_select(),
            Token::Insert => self.parse_insert(),
            Token::Update => self.parse_update(),
            Token::Delete => self.parse_delete(),
            token => Err(anyhow!("found token {token:?}")),
        }
    }

    fn parse_select(&mut self) -> anyhow::Result<Query> {
        let mut fields = Vec::new();
        loop {
            match self.bump()? {
                Token::Identifier(ident) => fields.push(ident),
                Token::Star => fields.push("*".to_string()),
                Token::From => break,
                token => return Err(anyhow!("expected FROM found {token:?}")),
            }
        }

        let table = match self.bump()? {
            Token::Identifier(table) => table,
            token => return Err(anyhow!("expected table name, found {token:?}")),
        };
        let filter = self.parse_filter()?;

        let zones = self.parse_zones()?;

        match self.bump()? {
            Token::Semicolon => (),
            token => return Err(anyhow!("expected ';', found {token:?}")),
        };

        Ok(Query::Select {
            table,
            fields,
            filter,
            zones,
        })
    }

    fn parse_insert(&mut self) -> anyhow::Result<Query> {
        match self.bump()? {
            Token::Into => (),
            token => return Err(anyhow!("expected INTO, found {token:?}")),
        };

        let table = match self.bump()? {
            Token::Identifier(name) => name,
            token => return Err(anyhow!("expected table name, found {token:?}")),
        };

        match self.bump()? {
            Token::LeftParen => (),
            token => return Err(anyhow!("expected '(', found {token:?}")),
        };

        let mut columns = Vec::new();
        loop {
            match self.bump()? {
                Token::Identifier(col) => columns.push(col),
                Token::RightParen => break,
                token => return Err(anyhow!("expected column name, found {token:?}")),
            }
        }

        match self.bump()? {
            Token::Values => (),
            token => return Err(anyhow!("expected VALUES, found {token:?}")),
        };

        match self.bump()? {
            Token::LeftParen => (),
            token => return Err(anyhow!("expected '(', found {token:?}")),
        };

        let mut values = Vec::new();
        loop {
            let mut record = Vec::new();
            loop {
                match self.bump()? {
                    Token::Number(num) => record.push(num.to_string()),
                    Token::Str(val) => record.push(val),
                    Token::True => record.push("true".to_string()),
                    Token::False => record.push("false".to_string()),
                    Token::RightParen => break,
                    token => return Err(anyhow!("expected value, found {token:?}")),
                }
            }
            values.push(record);

            match self.bump()? {
                Token::LeftParen => (),
                Token::Semicolon => break,
                token => return Err(anyhow!("expected '(', found {token:?}")),
            };
        }

        Ok(Query::Insert {
            table,
            columns,
            values,
        })
    }

    fn parse_update(&mut self) -> anyhow::Result<Query> {
        let table = match self.bump()? {
            Token::Identifier(name) => name,
            token => return Err(anyhow!("expected table name, found {token:?}")),
        };

        match self.bump()? {
            Token::Set => (),
            token => return Err(anyhow!("expected SET, found {token:?}")),
        };

        let mut assignments = Vec::new();
        loop {
            let column = match self.bump()? {
                Token::Identifier(name) => name,
                token => return Err(anyhow!("expected column name, found {token:?}")),
            };

            match self.bump()? {
                Token::Equal => (),
                token => return Err(anyhow!("expected '=', found {token:?}")),
            };

            let value = match self.bump()? {
                Token::Str(val) => val,
                Token::Number(num) => num.to_string(),
                token => return Err(anyhow!("expected literal value, found {token:?}")),
            };

            assignments.push((column, value));

            match self.first() {
                Some(token) if *token == Token::Where || *token == Token::Semicolon => break,
                Some(_) => continue,
                None => return Err(anyhow!("unterminated query")),
            };
        }

        let filter = self.parse_filter()?;

        let zones = self.parse_zones()?;

        match self.bump()? {
            Token::Semicolon => (),
            token => return Err(anyhow!("expected ';', found {token:?}")),
        };

        Ok(Query::Update {
            table,
            assignments,
            filter,
            zones,
        })
    }

    fn parse_delete(&mut self) -> anyhow::Result<Query> {
        match self.bump()? {
            Token::From => (),
            token => return Err(anyhow!("expected FROM, found {token:?}")),
        };

        let table = match self.bump()? {
            Token::Identifier(name) => name,
            token => return Err(anyhow!("expected table name, found {token:?}")),
        };

        let filter = self.parse_filter()?;

        let zones = self.parse_zones()?;

        match self.bump()? {
            Token::Semicolon => (),
            token => return Err(anyhow!("expected ';', found {token:?}")),
        };

        Ok(Query::Delete {
            table,
            filter,
            zones,
        })
    }

    fn parse_filter(&mut self) -> anyhow::Result<Option<Where>> {
        match self.first() {
            Some(token) if *token == Token::Semicolon => return Ok(None),
            Some(token) if *token == Token::Zone => return Ok(None),
            Some(token) if *token == Token::Where => self.bump()?,
            Some(token) => return Err(anyhow!("expected 'WHERE' found {token:?}")),
            None => return Ok(None),
        };

        let column = match self.bump()? {
            Token::Identifier(name) => name,
            token => return Err(anyhow!("expected column name, found {token:?}")),
        };

        let op = match self.bump()? {
            Token::Equal => "=".to_string(),
            // Token::Neq => "!=".to_string(),
            // Token::Lt => "<".to_string(),
            // Token::Gt => ">".to_string(),
            // Token::Le => "<=".to_string(),
            // Token::Ge => ">=".to_string(),
            token => return Err(anyhow!("expected operator, found {token:?}")),
        };

        let value = match self.bump()? {
            Token::Str(val) => val,
            Token::Number(num) => num.to_string(),
            token => return Err(anyhow!("expected literal value, found {token:?}")),
        };

        Ok(Some(Where { column, op, value }))
    }

    fn parse_zones(&mut self) -> anyhow::Result<Option<Vec<String>>> {
        match self.first() {
            Some(token) if *token == Token::Semicolon => return Ok(None),
            Some(token) if *token == Token::Zone => self.bump()?,
            Some(token) => return Err(anyhow!("expected 'ZONE' found {token:?}")),
            None => return Ok(None),
        };

        match self.bump()? {
            Token::Equal => (),
            token => return Err(anyhow!("expected '=', found {token:?}")),
        };

        match self.bump()? {
            Token::LeftBracket => (),
            token => return Err(anyhow!("expected '[', found {token:?}")),
        };

        let mut zones = Vec::new();
        loop {
            match self.bump()? {
                Token::Str(val) => zones.push(val),
                Token::Number(num) => zones.push(num.to_string()),
                Token::RightBracket => break,
                token => return Err(anyhow!("expected literal value, found {token:?}")),
            };
        }

        Ok(Some(zones))
    }

    fn bump(&mut self) -> anyhow::Result<Token> {
        self.tokens.pop().ok_or(anyhow!("bad query"))
    }

    fn first(&mut self) -> Option<&Token> {
        self.tokens.last()
    }
}
