# logoshot-text-search

### difflib_for_comparing_similar_strings.py
This is a file that is based on the python library, [difflib](https://github.com/python/cpython/blob/3.11/Lib/difflib.py), but modified to apply similarity comparing between Chinese characters.
The major difference between this new version of difflib and the original version of difflib from [cpython](https://github.com/python/cpython) is the definition of the matching block. If string a and b are to be compared, a character a[i] in string a will become a part of the matching block if it's similar enough to a character b[j] in string b, rather than requiring a[i] to be identical to some b[j]. The following bullet points are the changes that are made to the code according to this difference.
- Add similarity score
	- Since a matching block will be evaluated by using its similarity score, rather than using its size.
- Add similarity modes : glyph mode and sound(pinyin) mode
	- Users can define whether they want to compare the strings using the glyph or the sound.
  - To define the similarity, we use cosine similarity for glyph comparing and euclidean distance for sound comparing.
- Add threshold for similarity comparing
  - If the similarity between two characters, a[i] and b[j], is larger than the threshold, a[i] is then similar enough to b[j].
