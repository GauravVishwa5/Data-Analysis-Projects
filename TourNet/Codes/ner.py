# Sample code snippet
import spacy
from collections import Counter

nlp = spacy.load("TourNet/Datasets/tournet_social_data.csv")
location_entities = []

for text in df['post']:
    doc = nlp(text)
    location_entities += [ent.text for ent in doc.ents if ent.label_ in ['GPE', 'LOC']]

location_counts = Counter(location_entities).most_common(10)
