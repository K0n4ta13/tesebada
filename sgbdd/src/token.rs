#[derive(Debug, PartialEq, Clone)]
pub enum Token {
    LeftParen,
    RightParen,
    LeftBracket,
    RightBracket,
    Semicolon,
    Star,
    Equal,

    Identifier(String),
    Str(String),
    Number(f64),

    True,
    False,
    Select,
    Insert,
    Into,
    Values,
    Update,
    Set,
    Delete,
    From,
    Where,
    Zone,

    Eof,
}
