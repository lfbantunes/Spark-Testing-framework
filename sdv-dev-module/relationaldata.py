from sdv import load_demo, metadata
metadata, tables = load_demo(metadata=True)

from sdv.relational import HMA1

# model = HMA1(metadata)
# model.fit(tables)

# model.save('relational_model.pkl')
model = HMA1.load('relational_model.pkl')

new_data = model.sample()