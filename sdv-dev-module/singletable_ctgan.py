from sdv.demo import load_tabular_demo
from sdv.tabular import CTGAN

data = load_tabular_demo('student_placements_pii')
# data.head()

print("fitting")

model = CTGAN(
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
#model.save('ctgan_model.pkl')
#load the saved model
#loaded = CTGAN.load('ctgan_model.pkl')