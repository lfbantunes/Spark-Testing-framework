from sdv.demo import load_tabular_demo
from sdv.tabular import GaussianCopula

data = load_tabular_demo('student_placements_pii')
# data.head()

print("fitting")

model = GaussianCopula(
    primary_key='student_id',
    anonymize_fields={
        'address': 'address'
    },
    field_transformers={
        'address': 'label_encoding'
    }
)

model.fit(data)

print("fitted")

print("sampling")

new_data = model.sample(200)

print("done")

#saving the model created
#model.save('gauss_copula_model.pkl')
#load the saved model
#loaded = GaussianCopula.load('gauss_copula_model.pkl')