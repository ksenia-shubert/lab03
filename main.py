#SHUBERT_KSENIA_305


import subprocess


print("\n1/8 Downloading data from server")

#script_path = "first.py"
#subprocess.call(["python", script_path])

print("\n2/8 Creating tables")

script_path = "second.py"
subprocess.call(["python", script_path])

print("\n3/8 Convert files to .csv")

script_path = "third.py"
subprocess.call(["python", script_path])

print("\n4/8 Convert map files to .csv")

script_path = "fourth.py"
subprocess.call(["python", script_path])

print("\n5/8 Convert dataset to .csv")

script_path = "fifth.py"
subprocess.call(["python", script_path])

print("\n6/8 Creating tables from .csv")

script_path = "six.py"
subprocess.call(["python", script_path])

print("\n7/8 Export .csv files to postgresql server")

script_path = "seven.py"
subprocess.call(["python", script_path])

print("\n8/8 Making wordls map from table")

script_path = "eight.py"
subprocess.call(["python", script_path])

print("\nSuccessfully")
