//! Text helpers.

/// Split `text` into chunks of at most `max_bytes` UTF-8 bytes each,
/// preferring to break after a newline, then after whitespace, and only
/// otherwise mid-word (always on a character boundary). Leading/trailing
/// whitespace of each chunk is trimmed; empty chunks are dropped.
#[must_use]
pub fn split_message(text: &str, max_bytes: usize) -> Vec<String> {
    let max_bytes = max_bytes.max(4); // room for any single UTF-8 character
    let mut chunks = Vec::new();
    let mut rest = text.trim();
    while !rest.is_empty() {
        if rest.len() <= max_bytes {
            chunks.push(rest.to_owned());
            break;
        }
        let mut limit = max_bytes;
        while !rest.is_char_boundary(limit) {
            limit -= 1;
        }
        let window = &rest[..limit];
        let cut = window
            .rfind('\n')
            .or_else(|| window.rfind(char::is_whitespace))
            .filter(|&i| i > 0)
            .map_or(limit, |i| i + window[i..].chars().next().map_or(1, char::len_utf8));
        let chunk = rest[..cut].trim();
        if !chunk.is_empty() {
            chunks.push(chunk.to_owned());
        }
        rest = rest[cut..].trim_start();
    }
    chunks
}

/// Number of Unicode scalar values in `text` (what users perceive as length,
/// close enough for limits).
#[must_use]
pub fn char_len(text: &str) -> usize {
    text.chars().count()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_text_is_one_chunk() {
        assert_eq!(split_message("  hello  ", 100), vec!["hello"]);
        assert!(split_message("   ", 100).is_empty());
    }

    #[test]
    fn prefers_newlines_then_spaces() {
        let chunks = split_message("aaaa bbbb\ncccc dddd", 12);
        assert_eq!(chunks, vec!["aaaa bbbb", "cccc dddd"]);
        let chunks = split_message("aaaa bbbb cccc", 10);
        assert_eq!(chunks, vec!["aaaa bbbb", "cccc"]);
    }

    #[test]
    fn hard_splits_long_words_on_char_boundaries() {
        let word = "é".repeat(10); // 20 bytes
        let chunks = split_message(&word, 7);
        assert!(chunks.iter().all(|c| c.len() <= 7));
        assert_eq!(chunks.concat(), word);
    }

    #[test]
    fn every_chunk_respects_the_limit() {
        let text = "lorem ipsum dolor sit amet ".repeat(200);
        for chunk in split_message(&text, 2000) {
            assert!(chunk.len() <= 2000);
        }
    }
}
