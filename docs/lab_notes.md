# Tokenising and processing

Both GtR and CrunchBase include two variables with text project descriptions.

- GtR has an abstract and a technical abstract.
  - The technical abstract is only available for grants funded by the medical research council and BBSRC. Since these are less relevant for this project we have decided to ignore them
- CrunchBase has a 'long description' and a 'short description'
  - We create a new variable called "description" using the long description if it exists, and if not the short one
