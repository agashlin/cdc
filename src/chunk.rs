use super::{Separator};

pub struct Chunk {
    pub index: u64,
    pub size: u64,
    pub separator_hash: u64,
}

pub struct ChunkIter<Iter> {
    separators: Iter,
    stream_length: u64,
    last_separator_index: u64,
}

impl<Iter: Iterator<Item=Separator>> ChunkIter<Iter> {
    pub fn new(iter: Iter, stream_length: u64) -> ChunkIter<Iter> {
        ChunkIter {
            separators: iter,
            stream_length: stream_length,
            last_separator_index: 0,
        }
    }
}

impl<Iter: Iterator<Item=Separator>> Iterator for ChunkIter<Iter> {
    type Item = Chunk;

    fn next(&mut self) -> Option<Self::Item> {
        match self.separators.next() {
            Some(separator) => {
                let chunk_index = self.last_separator_index;
                let chunk_size = separator.index - self.last_separator_index;
                self.last_separator_index = separator.index;
                return Some(Chunk {
                    index: chunk_index,
                    size: chunk_size,
                    separator_hash: separator.hash,
                });
            },
            None => {
                let chunk_index = self.last_separator_index;
                let chunk_size = self.stream_length - self.last_separator_index;
                self.last_separator_index = self.stream_length;
                if chunk_size > 0 {
                    return Some(Chunk {
                        index: chunk_index,
                        size: chunk_size,
                        separator_hash: 0, // any value is ok, last chunk of the stream.
                    });
                }
                else {
                    return None;
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{ChunkIter, SeparatorIter};
    #[test]
    fn test_whole() {
        let bytes: Vec<u8> = (0..4096u32).map(|b| b as u8).collect();

        let chunks: Vec<_> = ChunkIter::new(
            SeparatorIter::custom_new(
                bytes.iter().cloned(),
                4,
                |hash| hash & 0xff == 0,
            ),
            bytes.len() as u64,
        ).collect();

        assert_eq!(chunks[0].index, 0);

        let mut i = 0;
        for chunk in &chunks {
            assert_eq!(chunk.index, i);
            i += chunk.size;
        }

        let last_chunk = &chunks[chunks.len()-1];
        assert_eq!((last_chunk.index + last_chunk.size) as usize, bytes.len());

    }
}
