import polars as pl
df = pl.DataFrame(
    {
        'Name': ['mbaye','moustapha','cheikh','patrick','aliou',
                    'charles','jean','jhon','anna','christine'],
        'email': ['mbaye@gmail.com','moustapha@gmail.com','cheikh@gmail.com', 'patrick@gmail.com','aliou@gmail.com',     
                  'charles@gmail.com','jean@gmail.com','jhon@gmail.com','anna@gmail.com','christine@gmail.com'],
        'salary en FCFA': [350000, 250000, 150000, 1300000, 175000, 500000, 900000, 850000, 175000, 235000]
    }
)
print(df) 
print(df.dtypes)