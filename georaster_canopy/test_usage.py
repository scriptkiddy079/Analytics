import psutil

print("CPU Usage %: ",psutil.cpu_percent())
print("Total Ram: ",psutil.virtual_memory().total / (1024 * 1024 * 1024), "GB")
print("Available Ram: ",psutil.virtual_memory().available / (1024 * 1024 * 1024), "GB")
print("Available Ram %: ",psutil.virtual_memory().available * 100 / psutil.virtual_memory().total)
