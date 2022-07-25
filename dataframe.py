import os

directory = '/lustre/cv/projects/casa/jsteeb/uc4/bda_data'
 
for filename in os.listdir(directory):
    f = os.path.join(directory, filename)
    if os.path.isfile(f):
        print(f)