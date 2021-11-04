import pandas as pd
from sdv.tabular import GaussianCopula

data = pd.read_csv('chinook_invoices.csv')
print(data)

# print("fitting")

# model = GaussianCopula(
#     primary_key='InvoiceId',
#     anonymize_fields={
#         'BillingAddress': 'address'
#     },
#     field_transformers={
#         'BillingAddress': 'label_encoding'
#     }
# )

# model.fit(data)

# print("fitted")

#saving the model created
# model.save('chinook_model.pkl')
#load the saved model
model = GaussianCopula.load('chinook_model.pkl')

print("sampling")

new_data = model.sample(200)

print("done")
print(new_data)

