from sdv import load_demo
from sdv import SDV

# 1 Model dataset

# Load example data
metadata, tables = load_demo(metadata=True)

# Fit a model using the SDV API
sdv = SDV()
sdv.fit(metadata, tables)

#saves leaned model for later use
sdv.save('sdv.pkl')


# 2 Sample data from fitted model

# Load fitted model. Not needed if usin single python session/script
sdv = SDV.load('sdv.pkl')

# Sample
samples = sdv.sample()

print("Original:")
print(tables["users"])
print()
print("New:")
print(samples["users"])