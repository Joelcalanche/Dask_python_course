"""
Parallel programming

Multi-threading

single python process , 2 or more cores


Parallel processing

2 o mas python processes, 2 o mas cores

Lazy evaluation

* Computations are not run until the moment the result is needed

(se crea una lista de tareas) 
* The steps required to compute the result are stored for later

* Dask splits the tasks  between threds or processes


Dask delayed

from dask import delayed

def my_square_function(x):
	return x**2

# Create delayed version of above function

delayed_square_function = delayed(my_square_function)

# Use the delayed function with input 4

delayed_result = delayed_square_function(4)

# Print the delayed answer


print(delayed_result)
# Delayed object


delayed_result = delayed(my_square_function)(4)

real_result = delayed_result.compute()# <- This line where the calculation happens

# Print the answer
print(real_result)

Using operations on  delayed objects


delayed_result1= delayed(my_square_function)(4)

# Math operations return delayed object

delayed_result2 = ( 4 + delayed_result1) * 5


# se guardan las operaciones, basicamente se trabaja con placesholders
print(delayed_result2.compute())

x_list = [30, 85, 14, 12, 27, 62, 89, 15, 78, 0]

sum_of_squares = 0


for x in x_list:
	# Square and add numbers
	sum_of_squares += delayed(my_square_function)(x) # la operacion puede ejecutarse en paralelo en processing o threads, by default is threads



result = sum_of_squares.compute()


# Print the aswer
print(result)


# Esta forma es ineficiente



Sharing computation

delayed_intermediate = delayed(my_square_function)(3)

# These two results both use delayed_intermediate

delayed_result1 = delayed_intermediate -5 

delayed_result2 = delayed_intermediate + 4


# delayed_3_squared will be computed twice


print('delayed_result1: delayed_result1.compute()')

print('delayed_result2:' delayed_result2.compute())




forma eficiente

import dask

# delayed_intermediate will be computed once

# 

comp_result1, comp_result2 = dask.compute(delayed_result1, delayed_result2)

print('comp_result1:', comp_result1)
print('comp_result2:', comp_result2)

# de esta forma se comparten resultados intermedios
"""




from dask import delayed

def fraction_to_percent(x):
     percentage = x * 100
     print('Converting to percentage')
     return x

frac = 0.3
percentage = delayed(fraction_to_percent)(frac)
computed_percentage = percentage.compute()

print(percentage)
print(computed_percentage)


"""
Delaying functions
You have been tasked with adding up your company's costs for the last 2 weeks. Because you know that you will want to run this same computation in the future with many more weeks, you think it might be a good idea to write it in a way that can be parallelized.

The arrays of costs of items for the last two weeks are available as costs_week_1 and costs_week_2, and numpy has been imported as np.


"""

# Import the delayed function from Dask
from dask import delayed

# Lazily calculate the sums of costs_week_1 and costs_week_2
sum1 = delayed(np.sum)(costs_week_1)
sum2 = delayed(np.sum)(costs_week_2)

# Add the two delayed sums
total = sum1  + sum2

# Compute and print the final answer
print(total.compute())


"""
task graphs and scheduling methods



Visualizing a task graph

# Create 2 delayed objects

delayed_num1 = delayed(my_square_function)(3)


delayed_num2 = delayed(my_square_function)(4)


# Add them

result = delayed_num1 + delayed_num2


# Plot the task graph


result.visualize()


Overlapping task graph

# Plot the task graph

dask.visualize(delayed_result1, delayed_result2)



Multi-threading vs. parallel processing

Moving data


Parallel processing
* Processes have their own RAM space

Multi-threading

* Threads use the same RAM space



# Run a sum on two big arrays

sum1 = delayed(np.sum)(big_array1)

sum2 = delayed(np.sum)(big_array2)



# Compute using processes

dask.compute(sum1, sum2)

* Slow using parallel processing


es lento por que los arrays de deben copiar instancias separadas y luego nuevamente copiados  a las seccion principal


# using Multi-threading

# Run a sum on two big arrays

sum1= delayed(np.sum)(big_array1)

sum2 = delayed(np.sum)(big_array2)


# Compute using threads

dask.compute(sum1, sum2)

Python have The GIL

Global interpreter lock - only one thread can read the Python script at a time

en parallel processing cada  cada proceso tiene su propio gil, puede leer codigo en parelelos

def sum_to_n(n);


#sums numbers from 0 to n 

	total = 0
	for i in range(n+1):
		total += i
	return total

* Multi-threading won't help here

* Parallel processing will

en este caso queremos correr algo multiples veces

e.g the pd.read_csv()function releases the GIl

df1 = delayed(pd.read_csv)('file1.csv')

df2 = delayed(pd.read_csv)('file2.csv')

Threads
* Are very fast to initiate

* Share memory space with main session

* No memory transfer needed

* Limited by the GIL, wich allows one thread to read the code at once


Processes
* Take time and memory to set up

* Have separate memory pools


* Very slow to transfer data between themselves and to the main Python session


* Each have their own GIL and so don't  need to take turns reading the code


*

"""
"""
Plotting the task graph
You are trying to analyze your company's spending records. Your manager wants to see what fraction of the total spending occurred in each month. But you are going to have to run it for many files, so it would be good to set up a lazy calculation so you can speed it up using threads or processes. To figure out which of these task scheduling methods might be better for this calculation, you would like to visualize the task graph.

The totals spent in two months are available for you as delayed objects as month_1_costs and month_2_costs. dask has also been imported for you.

"""


# Add the two delayed month costs
total_costs = month_1_costs + month_2_costs

# Calculate the fraction of total cost from month 1
month_1_fraction = month_1_costs/total_costs

# Calculate the fraction of total cost from month 2
month_2_fraction = month_2_costs/total_costs

# Plot the joint task graph used to calculate the fractions
dask.visualize(month_1_fraction, month_2_fraction)


"""

Building delayed pipelines

Parallel programming with dask in Python

"""
"""
Analyzing the data

import pandas as pd

maximuns = []


for file in files:
	# Load each file

	df = pd.read_csv(file)

	# Find maximum track  lenth in each file
	max_length = df['duration_ms'].max()

	# Store this maximum

	maximums.append(max_length)

# Find the maximun of all the maximun lengths

absolute_maximum = max(maximums)



version con dask more speed


import pandas as pd

maximums = []


for file in files:
	# Load each file
	df = delayed(pd.read_csv)(file)#<--------delay loading
	# Find maximum track length in each file

	max_length = df['duration_ms'].max()

	# Store this maximum

	maximums.append(max_length)


	# Find the maximum of all the maximum lengths


	absolute_maximum = delayed(max)(maximums)#<--------delay max() function

no puedes usar metodos internos hasta que no hays computado


import pandas as pd

maximums = []

for file in files:
	df = delayed(pd.read_csv)(file)

	# Use a method which doesn't exist

	max_length = df['duration_ms'].fake()

	maximums.append(max_length)

absolute_maximum = delayed(max)(maximumns)


esto dara error

# Compute all the maximuns

all_maximuns = dask.compute(maximuns)# retorna (lista), para obtener lista puedo usar indexing in 0


To delay or not to delay

def get_max_track(df):
	return df['duration_ms'].max()

for file in files:
	df = delayed(pd.read_csv)(file)
	# Use function to find max
	max_length = get_max_track(df)

	maximums.append(max_length)

absolute_maximum = delayed(max)(maximums)



"""
"""
Analyzing songs on Spotify
You have a list of CSV files that you want to aggregate to investigate the Spotify music catalog. Importantly, you want to be able to do this quickly and to utilize all your available computing power to do it.

Each CSV file contains all the songs released in a given year, and each row gives information about an individual song.

dask and delayed() have been imported for you, and the list of filenames is available in your environment as filenames. pandas has been imported as pd.

"""


n_songs_in_c, n_songs = 0, 0 

for file in filenames:
    # Load in the data
    df = delayed(pd.read_csv)(file)


    n_songs_in_c, n_songs = 0, 0 

for file in filenames:
    # Load in the data
    df = delayed(pd.read_csv)(file)
    
    # Add to running totals
    n_songs_in_c += (df['key'] == 'C').sum()


n_songs_in_c, n_songs = 0, 0 

for file in filenames:
    # Load in the data
    df = delayed(pd.read_csv)(file)
    
    # Add to running totals
    n_songs_in_c += (df['key'] == 'C').sum()
    print(n_songs_in_c)
    n_songs += df.shape[0]

# Efficiently compute total_n_songs_in_c and total_n_songs
total_n_songs_in_c, total_n_songs = dask.compute(n_songs_in_c, n_songs)

fraction_c = total_n_songs_in_c / total_n_songs
print(total_n_songs, fraction_c)



"""
How danceable are songs these days?
It's time to dive deeper into the Spotify data to analyze some trends in music.

In each CSV file, the 'danceability' column contains the score between 0 and 1 of how danceable each song is. The score describes how suitable a track is for dancing based on a combination of musical elements, including tempo, rhythm stability, beat strength, and overall regularity. Do you think songs are getting better or worse to dance to?

dask and the delayed() function have been imported for you. pandas has been imported as pd, and matplotlib.pyplot has been imported as plt. The list of filenames is available in your environment as filenames, and the year of each file is stored in the years list.

"""


danceabilities = []

for file in filenames:
	# Lazily load in the data
    df = delayed(pd.read_csv)(file)
    # Calculate the average danceability in the file of songs
    mean_danceability = df['danceability'].mean()
    danceabilities.append(mean_danceability)

# Compute all the mean danceabilities
danceability_list = dask.compute(danceabilities)[0]
# Plot the results
plt.plot(years, danceability_list)

plt.show()



"""
Most popular songs
You have one more task on this Spotify data, which is to find the top 10 most popular songs across all available years. The algorithm you will need to use to compute this is to calculate the top 10 songs in each year, and then combine these and find the top 10 of the top 10s.

The following function, which finds the top 10 songs in a DataFrame, has been provided for you and is available in your environment.

def top_10_most_popular(df):
  return df.nlargest(n=10, columns='popularity')
dask and the delayed() function have been imported for you. pandas has been imported as pd. The list of filenames is available in your environment as filenames, and the year of each file is stored in the list years.

"""


top_songs = []

for file in filenames:
    df = delayed(pd.read_csv)(file)
    # Find the top 10 most popular songs in this file
    df_top_10 = top_10_most_popular(df)
    top_songs.append(df_top_10)

# Compute the list of top 10s
# lista de dataframes
top_songs_list = dask.compute(top_songs)[0]
print(top_songs_list)

# Concatenate them and find the best of the best
top_songs_df = pd.concat(top_songs_list)
df_all_time_top_10 = top_10_most_popular(top_songs_df)
print("las mas top top")
print(df_all_time_top_10)





##----------------------------------------------------dask array



"""
NumPy vs. Dask arrays

import numpy as np

x = np.ones((4000, 6000))

print(x.sum()) takes 740 miliseconds
import dask.array as da

x = da.ones((4000, 6000),
			 chunks=(1000, 2000))# se le especifica el tama;o de los pedazos(chunks)

			 # takes 60 miliseconds to run


by default dask array using threads

Dask array methods

Dask arrays have almost all the methods that

NumPy arrays have

* x.max()
* x.min()

* x.sum()

* x.mean()

* etc
al ejecutar compute() me devuelve un array numpy


Treating Dask arrays like NumPy arrays


# Lazy mathematics with Dask array

y1 = x**2 + 2**x +1

# Lazy slicing

y2 = x[:10]

# Applying NumPy functions is lazy too
y3 = np.sin(x)



Loading arrays of images

import dask.array as da

import da.image

image_array = da.image.imread('images/*.png')


print(image_array)



Applying custom functions over chunks

def instagram_filter(image):
	return pretty_image

# Apply function to each image independently

pretty_image_array = image_array.map_blocks(instagram_filter) # aplica la funcion a un solo chunk
"""

"""
Dask array chunksizes
The Dask array x, which you can print, is available for you in the console. Using the array shape and chunk information, how many chunks of data make up the array?

"""

"""
In [1]:
x.shape
Out[1]:
(10000, 10000, 3)
In [2]:
x.chunks
Out[2]:

((2000, 2000, 2000, 2000, 2000),
 (1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000),
 (3,))
In [3]:
len(x.chunks)
Out[3]:
3


en este caso hay que estar pendiente es de contar las dimenciones

(5,10, 1)
"""


"""
Loading and processing photos
Let's say you are training a machine learning model to read American sign language images and translate them into text. The first part of this is loading images, and you have a lot of them. Thankfully, Dask can help you load and process these images lazily.




"""


# Import the image subpackage from dask.array
from dask.array import image

# Lazily load in all jpegs inside all subdirectories inside data/asl
image_array = image.imread('data/asl/*.jpeg')

# Load only the zeroth image into memory
zeroth_image = image_array[0].compute()

# Plot the image
plt.imshow(zeroth_image)
plt.show()

"""
An image processing pipeline
Your colleague has written a preprocessing function to use on the American sign language images in order to boost your machine learning model's accuracy. This function will take a grayscale image and run Canny edge detection on it. Canny edge detection is commonly used in classical computer vision and highlights the edges of objects in an image. You want to apply it to all of the images in your dataset.

The function your colleague has written is available in your environment as compute_edges(), and it takes an image that has dimensions (1, h, w) where the height h and the width w can be any integers.

The Dask array of your images is available in the environment as image_array. This array has shape (N, h, w, 3) where N is the number of images, and there are 3 channels for red, blue, and green.

dask.array has been imported for you as da.




"""


# Convert the color photos to grayscale
grayscale_images = image_array.mean(axis=-1)

# Apply the edge detection function
edge_images = grayscale_images.map_blocks(compute_edges)

# Select the zeroth image and compute its values
sample_image = edge_images[0].compute()

# Show the result
plt.imshow(sample_image, cmap='gray')
plt.show()




"""

Dask DataFrame


Pandas DataFrames vs. Dask DataFrame

import pandas as pd

# Read a single csv file

pandas_df = pd.read_csv('dataset/chunk1.csv')

This reads a single CSV file immediately

import dask.dataframe as dd
# Lazily read all csv files

dask_df = dd.read_csv("dataset/*.csv")

this reads all the CSV files in the dataset folder lazily

Controlling the size of blocks

# Set the maximum memory of a chunk
dask_df = dd.read_csv("dataset/*.csv", blocksize="10MB")

* Select column

col1 = dask_df['col1']

* Assigning columns

dask_df['double_col1'] = 2 * col1

* Mathematical operations

dask_df.std()
dask_df.min()

* Groupby
dask_df.groupby(col1).mean()

* Even functions you used earlier

dask_df.nlargest(n=3, columns='col1')




Datatimes and other pandas functionality

import pandas as pd

# Converting string to datetime format

pd.to_datetime(pandas_df['start_date'])


# Accessing datetime attributes

pandas_df['start_date'].dt.year

pandas_df['start_data'].dt.day

pandas_df['start_date'].dt.hour

pandas_df['start_date'].dt.minute

import dask.dataframe as dd

# Converting string to datetime format

dd.to_datetime(dask_df['start_date'])


# Accessing datetime attributes


dask_df['start_date'].dt.year

dask_df['start_date'].dt.day

dask_df['start_date'].dt.minute

Making results non-lazy
# show 5 rows

print(dask_df.head())



# Convert lazy Dask DataFrame to in-memory pandas DataFrame

results_df = df.compute(), esto retorna un pandas dataframes



# Conv


# 7 partitions (chunks) so 7 output files

dask_df.to_csv('answer/part-*.csv')


Faster file formats- Parquet

dask_df = dd.read_parquet('dataset_parquet')

# Save to parquet

dask_df.to_parquet('answer_parquet')


* Parquet format is multiple times faster to read data form than CSv

* Can be faste to write to 
"""

"""
Creating Dask dataframes from CSVs
Previously, you analyzed the Spotify song data using loops and delayed functions. Now you know that you can accomplish the same thing more easily using a Dask DataFrame. Let's see how much easier the same tasks you did earlier are if you do them using these methods instead of loops. First, however, you will need to load the dataset into a Dask DataFrame.

"""




# Import dask dataframe as dd
import dask.dataframe as dd

# Load in the DataFrame
df  = dd.read_csv("data/spotify/*.csv", blocksize="1MB")
 

# Convert the release_date column from string to datetime
df['release_date']=dd.to_datetime(df['release_date'])

# Show 5 rows of the DataFrame
print(df.head())


"""
Read Dask DataFrames from Parquet
In Chapter 1, you analyzed some Spotify data, which was split across multiple files to find the top hits of 2005-2020. You did this using the dask.delayed() function and a loop. Let's see how much easier this analysis becomes using Dask DataFrames.

dask.dataframe has been imported for you as dd.


"""

# Read the spotify_parquet folder
#dask_df = dd.read_parquet('dataset_parquet')
df = dd.read_parquet('data/spotify_parquet')

# Find the 10 most popular songs
# dask_df.nlargest(n=3, columns='col1')
top_10_songs = df.nlargest(n=10, columns='popularity')

# Convert the delayed result to a pandas DataFrame
top_10_songs_df = top_10_songs.compute()

print(top_10_songs_df)



"""
Summertime grooves
In chapter 1, you found that the average danceability of songs has been rising since 2005. Now, you are tasked with finding the average danceability of songs released in different months. Is there some part of the year where more danceable songs are released? If you were a musician who had written a new dance song, is there a best time of year to release that? Let's find out.


"""


# Extract the months from the release_date column using its datetime accessor 
months = df['release_date'].dt.month

# # Group the danceabilities by month
monthly_groupby = df['danceability'].groupby(months)

# # Find the mean danceability by month
monthly_danceability = monthly_groupby.mean()

# # Compute the result
monthly_danceability_result = monthly_danceability.compute()

monthly_danceability_result.plot()
plt.show()





#---------------------------------------------Multidimensional arrays

# Types of multi-dimensional data

"""
"Weather forecasts/observations"
* 3D biomedical scans

* Satellite images

* Data from other scientific instruments


HDF5 

* hierarchical Data Format

* Stored in hierarchical format - like (sub) directories



Navigating HDF5 files with h5py


import h5py


# Open the HDF5 file

file = h5py.File('data.hdf5')


# Print the available datasets inside the file

print(file.keys())


import h5py

# Open the HDF5 file

file = h5py.File('data.hdf5')


# Select dataset A

dataset_a = file["/A"]

print(dataset_a)

import dask.array as da

# Load dataset into a Dask array

a = da.from_array(dataset_a, chunks=(100, 20, 20))

print(a)


"""

"""
Zarr
* Hierarchical dataset like HDF5

* Designed to be chunked

* Good for streaming over cloud computing services like AWS, Google Cloud, etc


* Navigable file structure

import dask.array as da


a = da.from_zarr("dataset.zarr", component="A")

print(a)
"""

"""
In [3]:
file.keys()
Out[3]:
<KeysViewHDF5 ['latitude', 'longitude', 'month', 'precip', 'temp', 'year']>
In [4]:
print(file['/temp'])
<HDF5 dataset "temp": shape (508, 30, 45), type "<f4">

"""

"""
Dask arrays from HDF5 datasets
You have been tasked with analyzing European rainfall over the last 40 years. The monthly average rainfall in a grid of locations over Europe has been provided for you in HDF5 format. Since this file is pretty large, you decide to load and process it using Dask.

h5py has been imported for you, and dask.array has been imported as da.


Open the 'data/era_eu.hdf5' file using h5py.
Load the '/precip' variable into a Dask array using the from_array() function, and set chunks of (12 months, 15 latitudes, and 15 longitudes).
Use array slicing to select every 12th index along the first axis - this selects the January data from all years.
Take the mean of january_rainfalls along the time axis (axis 0) to calculate the mean rainfall in January across Europe.
"""

# Open the HDF5 dataset using h5py
hdf5_file = h5py.File("data/era_eu.hdf5")

# print(hdf5_file.keys())

# Load the file into a Dask array with a reasonable chunk size
precip = da.from_array(hdf5_file["/precip"], chunks=(12, 15, 15))


# Select only the months of January
january_rainfalls = precip[0::12]

# Calculate the mean rainfall in January for each location
january_mean_rainfall = january_rainfalls.mean(axis=0)

plt.imshow(january_mean_rainfall.compute())
plt.show()

"""
Dask arrays from Zarr datasets
You are tasked with analyzing European temperatures, and are given the same dataset which was in era_eu.hdf but this time in Zarr format. Zarr is a modern, powerful dataset format for storing chunked data. It is particularly good for use on cloud computing services but is also great on your own computer.

dask.array has been imported for you as da.


"""


# Load the temperature data from the Zarr dataset"
# a = da.from_zarr("dataset.zarr", component="A")
temps = da.from_zarr("data/era_eu.zarr", component="temp")

# Print the Dask array of temperatures to see the chunk sizes
print(temps)

# Find the minimum of the mean monthly temperatures
all_time_low = temps.min()

# Compute the answer
all_time_low_value = all_time_low.compute()

print(all_time_low_value, "°C")





#------------------------------------------------------------------------


"""
Xarray - like pandas in more dimensions


pandas
* Applies index labels to tabular data


Xarray
* Applies index labels to high dimensional arrays


# time and space


impor xarray as xr

ds = xr.open_zarr("data/era_eu.zarr")


<xarray.Dataset>

Dimensions: (lat: 30, lon: 45, time: 504)

Coordinates:
	*lat (lat) 
	*lon (lon)
	* time (time)

data variables:
	precip (time, lat, lon)

	temp (time, lat, lon)



DataFrame vs DataSet


pandas DataFrame

# Select a particular date

df.loc['2020-01-01']

# Select by index number

df.iloc[0]

# Select column

df['column1']

dask DataSet

# Select a particular date

ds.sel(time='20-01-01')

# Select by index number

ds.isel(time=0)

# Select variable

ds['variable1']


# Pandas dataFrame

# Perform mathematical operations

df.mean()


# Groupby and mean


df.groupby(df['time'].dt.year).mean()


# Rolling mean


rolling_mean = df.rolling(5).mean()

Dask DataSet

# Perform mathematical operations

ds.mean()

ds.mean(dim='dim1')

ds.mean(dim=('dim1', 'dim2'))


#Groupby and mean

ds.groupby(ds["time"].dt.year).mean()


# Rolling mean

rolling_mean = ds.rolling(dim1=5).mean()

"""

"""
Exploratory data analysis with xarray
Xarray makes working with multi-dimensional data easier, just like pandas makes working with tabular data easier. Best of all, Xarray can use Dask in the background to help you process the data quickly and efficiently.

You have been tasked with analyzing the European weather dataset further. Now that you know how to use Xarray, you will start by doing some exploratory data analysis.

xarray has been imported for you as xr.

"""


# Open the ERA5 dataset
ds = xr.open_zarr("data/era_eu.zarr")

# Select the zeroth time in the DataSet
ds_sel = ds.isel(time=0)

fig, (ax1, ax2) = plt.subplots(1,2, figsize=(8, 3))

# Plot the zeroth temperature field on ax1
ds_sel['temp'].plot(ax=ax1)

# Plot the zeroth precipitation field on ax2
ds_sel['precip'].plot(ax=ax2)
plt.show()




"""
Monthly mean temperatures
After seeing the analysis of the average temperatures, you simply need to see the rest of the months. This would be rather annoying to do using the method we used before as it involves a lot of complicated slicing in time to select the right months. Thankfully, with Xarray, it is a lot simpler.

The European weather dataset is available in your environment as the Xarray DataSet ds.

"""


# Extract the months from the time coordinates
months = ds['time'].dt.month

# Select the temp DataArray and group by months
monthly_groupby = ds['temp'].groupby(months)

# Find the mean temp by month
monthly_mean_temps = monthly_groupby.mean()

# Compute the result
monthly_mean_temps_computed = monthly_mean_temps.compute()

monthly_mean_temps_computed.plot(col='month', col_wrap=4, add_colorbar=False)
plt.show()


"""
Calculating the trend in European temperatures
You want to calculate the average European temperature from 1980 to present using the ERA5 dataset. This data is a Zarr dataset of monthly mean temperature and precipitation, on a grid of latitudes and longitudes. The Zarr file is chunked so that each subfile on the disk is an array of 15 latitudes, 15 longitudes, and 12 months.

xarray has been imported for you as xr.



"""


# Open the ERA5 dataset
ds = xr.open_zarr("data/era_eu.zarr")

# Select the temperature dataset and take the latitude and longitude mean
temp_timeseries = ds['temp'].mean(dim=('lat', 'lon'))

# Calculate the 12 month rolling mean
temp_rolling_mean = temp_timeseries.rolling(time=12).mean()

# Plot the result
temp_rolling_mean.plot()
plt.show()






#----------------------------------------------------------------------------------

"""


# Unstructured text data


string_list = ["Really good service....",
			   " This is the second time we've stayed...."]
			   "Great older hotel. My husband took..."


"""


# Semi-structured dictionary data

"""
dist_list = [

		{"name": "Beth", "employment": [{"role": "manager", "start_date": ...}]}
]



Dask bags

import dask.bag as db

# Create Dask bag from list

bag_example = db.from_sequence(string_list, npartitions=5)

print(bag_example)


# Print single element from bag

print(bag_example.take(1))# retorna una tuple
"""# si queremos cargar todo usamos .compute()

"""
Number of elements

number_of_elements = bag_example_count()

print(number_of_elements)

print(number_of_elements.compute())


Loading in text data
import glob

filenames = glob.glob('data/*.txt')

print(filenames)


text_data_bag = db.read_text(filenames)

text_data_bag = db.read_text('data/*.txt')

# by default 1 partition is created

String operactions

text_data_bag = db.read_text('data/*.txt')

print(text_data_bag.take(1))


# Convert the text to upper case

print(text_data_bag.str.lower().take(1))

# String operations

# change 'good' to 'great' in all places

print(text_data_bag.str.replace('good', 'great').take(1))

# How many times does 'great' appear the first 3 elements of the bag?

print(text_data_bag.str.count('great').take(3))

"""


"""
Creating a Dask bag
You have been tasked with analyzing some reviews left on TripAdvisor. Your colleague has provided the reviews as a list of strings. You want to use Dask to speed up your analysis of the data, so to start with, you need to load the data into a Dask bag.




"""
# Import the Dask bag subpackage as db
import dask.bag as db
# Convert the list to a Dask bag
review_bag = db.from_sequence(reviews_list, npartitions=3)

# Print 1 element of the bag
print(review_bag.take(1))

"""
Creating a bag from saved text
This time your colleague has saved the reviews to some text files. There are multiple files and multiple reviews in each file. Each review is on a separate line of the text file.

You want to load these into Dask lazily so you can use parallel processing to analyze them more quickly.

dask.bag has been imported for you as db.
"""

# Load in all the .txt files inside data/tripadvisor_hotel_reviews
review_bag = db.read_text('data/tripadvisor_hotel_reviews/*.txt')


# Count the number of reviews in the bag
review_count = review_bag.count()

# Compute and print the answer
print(review_count.compute())


"""
String operations
Now that you can load the text data into bags, it is time to actually do something with it. To detect how positive or negative the reviews are, you will start by counting some keywords.

The bag you created in the last exercise, review_bag, is available in your environment.




# Convert all of the reviews to lower case
lowercase_reviews = review_bag.str.lower()

# Count the number of times 'excellent' appears in each review
excellent_counts = lowercase_reviews.str.count('excellent')

# Print the first 10 counts of 'excellent'
print(excellent_counts.take(10))


"""
"""
Dask bag operations

The map method

def number_of_words(s):
	word_list = s.split(' ')
	return len(word_list)

print(number_of_words('these are four words'))

string_list = [
	'these are four words',
	'but these are five words',
	'and these are seven words in total'


]

# Create bag from list above

string_bag = db.from_sequence(string_list)

# Apply funtion to each element in bag

word_count_bag = string_bag.map(number_of_words)

# Run compute method

print(word_count_bag.compute())




JSON data


* inside an example json file, example_0.json

{"name":"beth", "employment:" [{"role":"manager", "start_date"}]}


text_bag = db.read_text('example*.json')


but this is just a string

Converting JSON from string to dictionary

text_bag = db.read_text('example*.json')

dict_bag = text_bag.map(json.loads)

print(dict_bag.take(1))# esto convierte a un json en un dictionario de python

Filtering

def is_new(employee_dict):
	# check if employee has less than 1 years services

	return employee_dict['years_service'] < 1


	# Select only the newer employees

	new_employee_bag = dict_bag.filter(is_new)


	# Count all employees and new employees

	print(dict_bag.count().compute(), new_employee_bag.count().compute())
	
Filtering

We can use a lambda function to do the same thing

def is_new(employee_dict):
	# Check if employee has less than 1 years service
	return employee_dict['years_service'] < 1

# Can use a lambda function instead of writing the function above

new_employee_bag = dict_bag.filter(lambda x: x['years_service'] < 1)

Pluck method
* inside an example JSON file, example_0.json

employment_bag = new_employee_bag.pluck('employment')
print(employment_bag.take(1))



pluck method

employment_bag = new_employee_bag.pluck('employment')

number_of_jobs_bag = employment_bag.map(len)

print(number_of_jobs_bag.take(1))


Aggregations

min_jobs = number_of_jobs_bag.min()

max_jobs = number_of_jobs_bag.max()

mean_jobs = number_of_jobs_bag.mean()

print(dask.compute(min_jobs, max_jobs, mean_jobs))



"""
"""
Loading JSON data
You have been asked to analyze some data about politicians from different countries. This data is stored in JSON format. The first step you need to accomplish is to load it in and convert it from string data to dictionaries.

dask.bag has been imported for you as db.

"""
# Import of the json package
import json

# Read all of the JSON files inside data/politicians
text_bag = db.read_text('data/politicians/*.json')

# Convert the JSON strings into dictionaries
dict_bag = text_bag.map(json.loads)

# Show an example dictionary
print(dict_bag.take(1))




"""
Filtering Dask bags
The politician data you are working with comes from different sources, so it isn't very clean. Many of the dictionaries are missing keys that you may need to run your analysis. You will need to filter out the elements with important missing keys.

A function named has_birth_date() is available in the environment. It checks the input dictionary to see if it contains the key 'birth_date'. It returns True if the key is in the dictionary and False if not.

def has_birth_date(dictionary):
  return 'birth_date' in dictionary
The bag you created in the last exercise is available in your environment as dict_bag.


"""
# Print the number of elements in dict_bag
print(dict_bag.count().compute())

# # Filter out records using the has_birth_date() function
filtered_bag = dict_bag.filter(has_birth_date)

# # Print the number of elements in filtered_bag
print(filtered_bag.count().compute())



"""
Chaining operations
Now that you have loaded and cleaned the data, you can begin analyzing it. Your first task is to look at the birth dates of the politicians. The birth dates are in string format like 'YYYY-MM-DD'. The first 4 characters in the string are the year.

The filtered Dask bag you created in the last exercise, filtered_bag, is available in your environment.

"""

# Select the 'birth_date' from each dictionary in the bag
birth_date_bag = filtered_bag.pluck('birth_date')

# Extract the year as an integer from the birth_date strings
birth_year_bag = birth_date_bag.map(lambda x: int(x[:4]))

# Calculate the min, max and mean birth years
min_year = birth_year_bag.min()
max_year = birth_year_bag.max()
mean_year = birth_year_bag.mean()

# Compute the results efficiently and print them
print(dask.compute(min_year, max_year, mean_year))



"""
Converting 

unstructured data to a DataFrame

Restructuring a dictionary



def add_number_of_jobs(employee_dict):

	employee_dict['number_of_previos_jobs'] = len(employee_dict['employment'])

	return employee_dict

dict_bag = dict_bag.map(add_number_of_jobs)



Removing parts of the dictionary

def delete_dictionary_entry(dictionary, key_to_drop):

		del dictionary[key_to_drop]
		return dictionary

dict_bag = dict_bag.map(delete_dictionary_entry, key_to_drop='employment')


Selecting parts of the dictionary

def filter_dictionary(dictionary, keys_to_keep):

	new_dict = {}

	for k in keys_to_keep:
		new_dict[k] = dictionary[k]
	return new_dict

dict_bag = dict_bag.map(
				filter_dictionary,
				keys_to_keep = ['name', 'number_of_previous_jobs']
	)


print(dict_bag.take(1))

converted_bag_df = dict_bag.to_dataframe()

print(converted_bag_df)
"""

"""
Restructuring a dictionary
Now you want to clean up the politician data and move it into a Dask DataFrame. However, the politician data is nested, so you will need to process it some more before it fits into a DataFrame.

One particular piece of data you want to extract is buried a few layers inside the dictionary. This is a link to a website for each politician. The example below shows how it is stored inside the dictionary.

record = {
...
 'links': [{'note': '...',
            'url': '...'},],  # Stored here
...
}
The bag of politician data is available in your environment as dict_bag.

"""
def extract_url(x):
    # Extract the url and assign it to the key 'url'
    x['url'] = x["links"][0]['url']
    return x
  
# Run the function on all elements in the bag.
dict_bag = dict_bag.map(extract_url)

print(dict_bag.take(1))


"""
Converting to DataFrame
You want to make a DataFrame out of the politician JSON data. Now that you have de-nested the data, all you need to do is select the keys to keep as columns in the DataFrame.

The Dask bag you created in the last exercise is available in your environment as dict_bag

"""


def select_keys(dictionary, keys_to_keep):
  new_dict = {}
  # Loop through kept keys and add them to new dictionary
  for k in keys_to_keep:
    new_dict[k] = dictionary[k]
  return new_dict

# Use the select_keys to reduce to the 4 required keys
filtered_bag = dict_bag.map(select_keys, keys_to_keep=['gender','name', 'birth_date', 'url'])

# Convert the restructured bag to a DataFrame
df = filtered_bag.to_dataframe()


# Print the first few rows of the DataFrame
print(df.head())


"""
Using any data in Dask bags


Complex mixed data format, example video


Creating a Dask bag

import glob

video_filenames = glob.glob("*.mp4")

print(video_filenames)


#then

import dask.bag as db

filename_bag = db.from_sequence(video_filenames)

filename_bag.take(1)[0]

# Loading custom data

load_mp4("video.mp4")

data_bag = filename_bag.map(load_mp4)

data_bag.take(1)[0]


data_bag = filename_bag.map(load_mp4)

# Create empty list

data_list = []


# Add delayed loaded files to list

for file in video_filenames:
	data_list.append(dask.delayed(load_mp4)(file))

List of delayed objects vs. Dask bag

# Convert list of delayed objects to dask bag

data_bag = db.from_delayed(data_list)


# Conver dask bag to list of delayed objects

data_list = data_bag.to_delayed()


Further analysis

transcribed_bag = data_bag.map(transcribe_audio)

transcribe_bag.take(1)[0]

Futher analysis

# Apply custom function to remove videos with no spoken words

clean_bag = transcribed_bag.filter(transcript_is_not_blank)


# Apply sentiment analysis to transcripts

sentiment_bag = clean_bag.map(analyze_transcript_sentiment)


# Remove unwanted elements from bag

keys_to_drop = ['video', 'audio']

final_bag = sentiment_bag.map(filter_dictionary, keys_to_drop=keys_to_drop)


# Convert to dask DataFrame

df = final_bag.to_dataframe()

df.compute()# to get Results



Using .wav files


# import scipy module for .wav files

from scipy.io import wavfile

# load sampling frequency and audio array


sample_freq, audio = wavfile.read(filename)

# Samples per second

print(sample_freq) # 44100 en khz


# The audio data

print(audio)

# array[148, 142, 159,.....] array de amplitudes

"""
"""
Loading wav data
To work with any non-standard data using Dask bags, you will need to write a lot of functions yourself. For this task, you are analyzing audio data, and so you need a custom function to load it.

Some of the audio recordings failed, and the audio is silent in these. Regular audio data looks like a wave, where the amplitude goes to large positive and negative values. Therefore, to check if a recording is silent, you can check whether the audio clip has very small amplitudes overall.

The scipy.io.wavfile module has been imported into your environment as wavfile, and numpy has been imported as np.


"""
def load_wav(filename):
    # Load in the audio data
    sampling_freq, audio = wavfile.read(filename)
    
    # Add the filename, audio data, and sampling frequency to the dictionary
    data_dict = {
        'filename': filename,
        'audio': audio, 
        'sample_frequency': sampling_freq
    }
    return data_dict

def not_silent(data_dict):
    # Check if the audio data is silent
    return np.mean(np.abs(data_dict['audio'])) > 100


"""
Constructing custom Dask bags
A common use case for Dask bags is to convert some code you have already written to run in parallel. Depending on the code, sometimes it can be easier to construct lists of delayed objects and then convert them to a bag. Other times it will be easier to form a Dask bag early on in the code and map functions over it. Which of these options is easier will depend on your exact code, so it's important that you know how to use either method.

dask has been imported for you, and dask.bag has been imported as db. A list of file names strings is available in your environment as wavfiles.


"""

# Convert the list of filenames into a Dask bag
filename_bag = db.from_sequence(wavfiles)

# Apply the load_wav() function to each element of the bag
loaded_audio_bag =  filename_bag.map(load_wav)


delayed_loaded_audio = []

for wavfile in wavfiles:
    # Append the delayed loaded audio to the list
    delayed_loaded_audio.append(dask.delayed(load_wav)(wavfile))

# Convert the list to a Dask bag
loaded_audio_bag = db.from_delayed(delayed_loaded_audio)



"""
Processing unstructured audio
You have a lot of .wav files to process, which could take a long time. Fortunately, the functions you just wrote can be used with Dask bags to run the analysis in parallel using all your available cores.

Here are descriptions of the not_silent() function you wrote, plus two extras you can use.

not_silent(audio_dict) - Takes an audio dictionary, and checks if the audio isn't silent. Returns True/False.
peak_frequency(audio_dict) - Takes a dictionary of audio data, analyzes it to find the peak frequency of the audio, and adds it to the dictionary.
delete_dictionary_entry(dict, key_to_drop) - Deletes a given key from the input dictionary.
The audio data loaded_audio_bag is available in your environment.


"""

# Filter out blank audio files
filtered_audio_bag = loaded_audio_bag.filter(not_silent)

# Apply the peak_frequency function to all audio files
audio_and_freq_bag = filtered_audio_bag.map(peak_frequency)

# Use the delete_dictionary_entry function to drop the audio
final_bag = audio_and_freq_bag.map(delete_dictionary_entry, key_to_drop='audio')

# Convert to a DataFrame and run the computation
df = final_bag.to_dataframe().compute()
print(df)




#-----------------------------------------------------------------

"""

Using processes and threads



Dask default scheduler


Threads
* Dask arrays
* Dask DataFrame

* Delayed pipelines created with

dask.delayed()

Processes
* Dask bags

Choosing the scheduler

# Use default 

result = x.compute()
result = dask.compute(x)

# Use threads

result = x.compute(scheduler='threads')

result = dask.compute(scheduler='threads')




# Use processes


result = x.compute(scheduler='processes')

result = dask.compute(x, scheduler='processes')

Recap - threads vs.  processes


Threads
* Are very fast to initiate

* No need to transfer data to them

* Are limited by the GIL, which allows one thread to read te code at once

Processes
* Take time to set up
* Slow to transfer data to
* Each have their own GIL and so don't need to take turns reading the code



Creating a local cluster

from dask.distributed import LocalCluster


cluster = LocalCluster(
	processes=True,
	n_workers=2,
	threads_per_worker=2

)


print(cluster)


from dask.distributed import LocalCluster

cluster = LocalCluster(
	processes = False,
	n_workers =2,
	threads_per_worker =2
)

print(cluster)


SImple local cluster

cluster = LocalCluster(processes=True)

print(cluster)
# LocalCluster(...,worker=4, threads=8)

cluster = LocalCluster(processes=False)

print(cluster)
# LocalCluster(..., workers=1, threads=8)





Creating a client


from dask.distributed import Client, LocalCluster


cluster = LocalCluster(
	processes=True,
	n_workers=4,
	threads_per_worker=2

)


client = Client(cluster)

print(client)



Create cluster then pass it into client


cluster = LocalCluster(
	processes=True,
	n_workers=4,
	threads_per_worker=2
)

client = Client(cluster)

print(client)

Create client which will create its own cluster

client = Client(
	processes=True,
	n_workers=4,
	threads_per_worker =2
)

print(client)

Using the cluster

client =  Client(processes=True)

# Default uses the client

result = x.compute()

# Can still change to other schedulers

result= x.compute(scheduler='threads')

# Can explicity use client

result = client.compute(x)


Other kinds of cluster

* LocalCluster - A cluster on your computer

* Other cluster types split computation across different computers
"""


"""
Clusters and clients
Depending on your computer hardware and the calculation you are trying to complete, it may be faster to run it using a mixture of threads and processes. To do this, you need to set up a local cluster.

There are two ways to set up a local cluster that Dask will use. The first way is to create the local cluster and pass it to a client. This is very similar to how you would set up a client to run across a cluster of computers! The second way is to use the client directly and allow it to create the local cluster itself. This is a shortcut that works for local clusters, but not for the other types of cluster.

In this exercise, you will create clients using both methods.

Be careful when creating the cluster and clients. If you configure them incorrectly, your session may time out.


"""

# Import Client and LocalCluster
from dask.distributed import Client, LocalCluster

# Create a thread-based local cluster
cluster = LocalCluster(
	processes=False,
    n_workers=4,
    threads_per_worker=1,
)

# Create a client
client = Client(cluster)




"""
"""
from dask.distributed import Client

# Create a client without creating cluster first
client = Client(
	processes=False, 
    n_workers=4,
    threads_per_worker=1
)

















##----------------------------------------------------------------------------


"""
Training machine learning models on big datasets

Dask-Ml

import dask_ml
* Speeds up machine learning tasks

Fitting a linear regression

# import regression model

from sklearn.linear_model import SGDRegressor

# Create instance of model

model = SGDRegressor()

# Fit model to data

model.fit(X, y)


# Make predictions

y_pred = model.predit(X)


# What if the data is too big?
# What if we want to fit the model to dask DataFrames, or arrays

Dask-ml allows to fit scikit-learn models to lazy datasets
"""


# import regression model

from skelearn.linear_model import SGDRegressor

# Create instance of model
model = SGDRegressor()

# Import Dask-ML wrapper for model

from dask_ml.wrappers import Incremental


# Wrap model

dask_model = Incremental(model, scoring='neg_mean_squared_error')

# Fit on Dask dataFrames or arrays

dask.model.fit(dask_X, dask_y) # not lazy

"""
training an incremental model

for i in range(10):
	dask_model.partial_fit(dask_X, dask_y) # not lazy

"""
for i in range(10):
	dask_model.partial_fit(dask_X, dask_y) # not lazy

"""
Generating predictions

y_pred = dask.model.predict(dask_X)

print(y_pred)

print(y_pred.compute())
"""
"""
Using Dask to train a linear model
Dask can be used to train machine learning models on datasets that are too big to fit in memory, and allows you to distribute the data loading, preprocessing, and training across multiple threads, processes, and even across multiple computers.

You have been tasked with training a machine learning model which will predict the popularity of songs in the Spotify dataset you used in previous chapters. The data has already been loaded as lazy Dask DataFrames. The input variables are available as dask_X and contain a few numeric columns, such as the song's tempo and danceability. The target values are available as dask_y and are the popularity score of each song.
"""
from sklearn.linear_model import SGDRegressor
from dask_ml.wrappers import Incremental

# Create a SGDRegressor model
model = SGDRegressor()

# Wrap the model so that it works with Dask
dask_model =  Incremental(model, scoring='neg_mean_squared_error')

# Fit the wrapped model
dask_model.fit(dask_X, dask_y)



"""
Making lazy predictions
The model you trained last time was good, but it could be better if you passed through the training data a few more times. Also, it is a shame to see a good model go to waste, so you should use this one to make some predictions on a separate dataset from the one you train on.

An unfitted version of the model you created in the last exercise is available in your environment as dask_model. Dask DataFrames of training data are available as dask_X and dask_y.


"""
# Loop over the training data 5 times"


"""
for i in range(10):
	dask_model.partial_fit(dask_X, dask_y) # not lazy
"""
for i in range(5):
	dask_model.partial_fit(dask_X, dask_y)

# Use your model to make predictions
y_pred_delayed = dask_model.predict(dask_X)

# Compute the predictions
y_pred_computed = y_pred_delayed.compute()

print(y_pred_computed)










####---------------------------------------------------------------------


"""
Machine learning with Big data set

"""


"""
# Load tabular dataset

import dask.dataframe as dd

dask_df = dd.read_parquet("data_parquet")

X = dask_df[["feature1", "feature2", "feature3"]]

y = dask_df['target_column']


from dask_ml.preprocessing import StandardScaler

scaler = StandardScaler()

scaler.fit(X) # This is not lazy

standardized_X = scaler.transform(X) # This is lazy

from das_ml.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(X, y, shuffle=True, test_size=0.2)

print(X_train)


Scoring

# Test the fit model on training data

train_score = dask_model.score(X_train, y_train) # Not lazy

print(train_score)

# test the fit model on testing data

test_score = dask_model.score(X_test, y_test) # Not lazy

print(test_score)
"""

"""
Lazily transforming training data
Preprocessing your input variables is a vital step in machine learning and will often improve the accuracy of the model you create. In the last couple of exercises, the Spotify data was preprocessed for you, but it is important that you know how to do it yourself.

In this exercise, you will use the StandardScaler() scaler object, which transforms columns of an array so that they have a mean of zero and standard deviation of one.

The Dask DataFrame of Spotify songs is available in your environment as dask_df. It contains both the target popularity scores and the input variables which you used to predict these scores.

"""

# Import the StandardScaler class
from dask_ml.preprocessing import StandardScaler

X = dask_df[['duration_ms', 'explicit', 'danceability', 'acousticness', 'instrumentalness', 'tempo']]

# Select the target variable
y = dask_df['popularity']

# Create a StandardScaler object and fit it on X
scaler = StandardScaler()
scaler.fit(X)

# Transform X
X = scaler.transform(X)
print(X)


"""
Lazy train-test split
You have transformed the X variables. Now you need to finish your data prep by transforming the y variables and splitting your data into train and test sets.

The variables X and y, which you created in the last exercise, are available in your environment.
"""

# Import the train_test_split function
from dask_ml.model_selection import train_test_split

# Rescale the target values
y = y/100

# Split the data into train and test sets
X_train, X_test, y_train, y_test = train_test_split(X, y, shuffle=True, test_size=0.2)

print(X_train)