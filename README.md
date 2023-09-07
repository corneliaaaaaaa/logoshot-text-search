# logoshot-text-search

### Introduction of Logoshot
Logoshot is an application which allows users to find similar trademarks based on multiple criteria, including logo designs, trademark names, application date and other details of logos. It can be used for the following two purposes.
* Intellectual property management & inspection for the government, attorneys or companies which plan to publish new logos.
* Simple search engine for the public when they run into a new trademark, but can't remember the exact name of the trademark.

### Introduction of backend components
The backend of Logoshot can be separated into the following two components.
* Image recognition: allows users to take a photo of a logo, and the system will return trademarks which looks like the image.
* Text search: allows users to input a keyword and other search criteria of the trademark, and the system will return trademarks which are both similar to the keyword and matches the search criteria.

This repository mainly contains the text search component of Logoshot's backend.
