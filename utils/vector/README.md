### glyph
**glyph** is a module for computing similarity between the glyph of different Chinese characters. The module first turns Chinese characters into binary vectors, whose dimension represents whether the character contains the corresponding component. Then, the similarity equals to the cosine similarity between the two vectors.
### pinyin
**pinyin** is a module for computing similarity between the sounds of different Chinese characters. The module is based on dimsim, the library developed by IBM, but modified to apply more flexible vector computation.