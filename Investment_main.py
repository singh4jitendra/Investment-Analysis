
# coding: utf-8

# # Table 1.1

# In[142]:


# %matplotlib inline
import pandas as pd
import csv
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib.ticker import FuncFormatter
import matplotlib.ticker as ticker
import warnings

warnings.simplefilter('ignore')



# Method to replace '0' with 'na'/'Na' appropriately
def replace_0_with_na(s):
    s = str(s)
    if(s.startswith("0")):
        s =s.replace("0","Na")
    elif(s.count("0") >0 and s.index("0") >0):
        s= s.replace("0","na")
    return s


#Method to convert value to Billion
def billions(x, pos):
    return '$%1.1fB' % (x*1e-9)




#Read data from companies.txt
comp = pd.read_csv(r".\data\companies.txt" , encoding ='ISO-8859-1',delimiter = "\t")

#Read data from rounds2.csv
rounds2 = pd.read_csv(r".\data\rounds2.csv" , encoding ='ISO-8859-1')

#Making all values of 'company_permalink' column to lowercase to count unique values
uniq_comp_in_rounds2 = rounds2['company_permalink'].map(lambda x: x.split("/")[2].lower())



#Q.1 How many unique companies are present in companies?
print("\nunique companies are present in companies : %d" %(comp.permalink.nunique()))

#Q.2 How many unique companies are present in rounds2?
print("\nunique companies are present in rounds2: %d" %(uniq_comp_in_rounds2.nunique()))

#Q.3 In the companies data frame, which column can be used as the unique key for each company? Write the name of the column.
print("Unique Column : permalink")


#Q.4 Are there any companies in the rounds2 file which are not present in companies? Answer yes or no: Y/N
comp_in_rounds2_and_not_in_comp = (uniq_comp_in_rounds2.nunique() -comp.permalink.nunique())
if(comp_in_rounds2_and_not_in_comp > 0):
    print("Yes, we have 2 companies in the rounds2, which is not in companies.txt")


#Q.5 Merge the two data frames so that all variables (columns).

#Renaming company_permalink to permalink for ease of merge
rounds2.rename(columns={'company_permalink': 'permalink'}, inplace=True)
comp['permalink'] = comp['permalink'].str.lower()
rounds2['permalink'] = rounds2['permalink'].str.lower()
master_frame = pd.merge(comp, rounds2,  on='permalink', how="outer")
print("No. of observaitons : %s " %(master_frame.shape[0]))




"""
Table 2.1
""" 


#Filtering with funding_round_type column
df_funding = master_frame["funding_round_type"]

total_venture =  master_frame[df_funding == "venture"].shape[0]
total_angel =  master_frame[df_funding == "angel"].shape[0]
total_seed =  master_frame[df_funding == "seed"].shape[0]
total_private_equity =  master_frame[df_funding == "private_equity"].shape[0]


master_frame_with_total_raised_amount_sum_for_all_type_of_funding = master_frame.groupby('funding_round_type').raised_amount_usd.mean()
print("Average_ventur         : %.2f" %(master_frame_with_total_raised_amount_sum_for_all_type_of_funding["venture"]))
print("Average_angel          : %.2f" %(master_frame_with_total_raised_amount_sum_for_all_type_of_funding["angel"]))
print("Average_seed           : %.2f" %(master_frame_with_total_raised_amount_sum_for_all_type_of_funding["seed"]))
print("Average_private_equity : %.2f" %(master_frame_with_total_raised_amount_sum_for_all_type_of_funding["private_equity"]))



#Considering that Spark Funds wants to invest between 5 to 15 million USD per investment round, 
#which investment type is the most suitable for it?

USD_5M=master_frame["raised_amount_usd"] >= 500000
USD_15M=master_frame["raised_amount_usd"] <= 1500000
df_5M_15M_USD = master_frame[(USD_5M) & (USD_15M)]

most_suitable_investment_type =  df_5M_15M_USD.groupby('funding_round_type').raised_amount_usd.sum().idxmax()
print("Most suitable investment type : %s" %(most_suitable_investment_type))



""" 
Table 3.1
"""


venture_master_frame = master_frame[df_funding == "venture"]

venture_master_frame_with_total_raised_amount_sum = venture_master_frame.groupby('country_code').raised_amount_usd.sum()
top_9_countries = venture_master_frame_with_total_raised_amount_sum.sort_values(ascending=False).head(9)

print(top_9_countries[['USA', 'GBR', 'IND']]) 


"""
Table 5.1
"""

########### Extract the primary sector of each category list from the category_list column and putting in master frame
 
mapping = pd.read_csv(r".\data\mapping.csv" , encoding ='ISO-8859-1')
df_primary_sector = master_frame['category_list'].apply(lambda x: str(x).split('|')[0])
master_frame["primary_sector"] = pd.DataFrame(df_primary_sector.values)


############# Use the mapping file 'mapping.csv' to map each primary sector to one of the eight main sector 

#reversing one-hot encoding
df_mapping_with_main_sector = mapping
df_mapping_with_main_sector["main_sector"] = pd.get_dummies(mapping).idxmax(1)
df_mapping_with_main_sector.drop(mapping.columns[[1,2,3,4,5,6,7,8,9]], axis = 1, inplace = True) 


###############  Removing '0' from category_list e.g. A0lytics=>Analytics ###########
master_frame["category_list"] = master_frame["category_list"].apply(replace_0_with_na)
df_mapping_with_main_sector["category_list"] = df_mapping_with_main_sector["category_list"].apply(replace_0_with_na)


###############  Mapping main_sector in master_Frame from mapping frame  ###########
master_frame['main_sector'] = master_frame['primary_sector'].map(df_mapping_with_main_sector.set_index('category_list')['main_sector'])



################## Table 5.1 ###################

venture_master_frame_with_main_sector = master_frame[df_funding == "venture"]
venture_master_frame_with_main_sector_with_5M_and15M_inestment = venture_master_frame_with_main_sector[(USD_5M2) & (USD_15M2)]
df_country = venture_master_frame_with_main_sector_with_5M_and15M_inestment["country_code"]
D1 =  venture_master_frame_with_main_sector_with_5M_and15M_inestment[df_country == "USA"]
D2 =  venture_master_frame_with_main_sector_with_5M_and15M_inestment[df_country == "GBR"]
D3 =  venture_master_frame_with_main_sector_with_5M_and15M_inestment[df_country == "IND"]



#####################################
#The total number (or count) of investments for each main sector in a separate column
#
#The total amount invested in each main sector in a separate column
##################################


D1_total_count_invested_per_sector = pd.DataFrame({'count' : D1.groupby( "main_sector" ).raised_amount_usd.size()}).reset_index()
D1_total_amount_invested_per_sector = pd.DataFrame({'Sum' : D1.groupby( "main_sector" ).raised_amount_usd.sum()}).reset_index()
D1['total_count_invested_per_sector'] = D1['main_sector'].map(D1_total_count_invested_per_sector.set_index('main_sector')['count'])
D1['total_amount_invested_per_sector'] = D1['main_sector'].map(D1_total_amount_invested_per_sector.set_index('main_sector')['Sum'])

D2_total_count_invested_per_sector = pd.DataFrame({'count' : D2.groupby( "main_sector" ).raised_amount_usd.size()}).reset_index()
D2_total_amount_invested_per_sector = pd.DataFrame({'Sum' : D2.groupby( "main_sector" ).raised_amount_usd.sum()}).reset_index()
D2['total_count_invested_per_sector'] = D2['main_sector'].map(D2_total_count_invested_per_sector.set_index('main_sector')['count'])
D2['total_amount_invested_per_sector'] = D2['main_sector'].map(D2_total_amount_invested_per_sector.set_index('main_sector')['Sum'])

D3_total_count_invested_per_sector = pd.DataFrame({'count' : D3.groupby( "main_sector" ).raised_amount_usd.size()}).reset_index()
D3_total_amount_invested_per_sector = pd.DataFrame({'Sum' : D3.groupby( "main_sector" ).raised_amount_usd.sum()}).reset_index()
D3['total_count_invested_per_sector'] = D3['main_sector'].map(D3_total_count_invested_per_sector.set_index('main_sector')['count'])
D3['total_amount_invested_per_sector'] = D3['main_sector'].map(D3_total_amount_invested_per_sector.set_index('main_sector')['Sum'])

######################## Table 5.1 #########################
print("\n############ Country-1 #############\n")
print("Total number of investments %s " %(D1.shape[0]))
print("Total amount of investment %s" %(D1.raised_amount_usd.sum())) 

all_sector_value_count = D1['main_sector'].value_counts()
top_Sector = all_sector_value_count.index[0]
second_best_Sector = all_sector_value_count.index[1]
third_best_Sector = all_sector_value_count.index[2]


print("Top sector (based on count of investments) %s" %(top_Sector))
print("Second best sector (based on count of investments) %s" %(second_best_Sector))
print("Third best sector (based on count of investments) %s" %(all_sector_value_count.index[2]))
print("Number of investments in the top sector : %s " %(all_sector_value_count.iloc[0]))
print("Number of investments in the second best sector : %s " %(all_sector_value_count.iloc[1]))
print("Number of investments in the third best sector : %s " %(all_sector_value_count.iloc[2]))
print("For top sector,company received the highest investment :%s" %(D1[D1["main_sector"] == top_Sector].groupby("name")["raised_amount_usd"].sum().idxmax()))
print("For 2nd best sector,company received the highest investment :%s" %(D1[D1["main_sector"] == second_best_Sector].groupby("name")["raised_amount_usd"].sum().idxmax()))



print("\n############ Country-2 #############\n")
print("Total number of investments %s " %(D2.shape[0]))
print("Total amount of investment %s" %(D2.raised_amount_usd.sum())) 

D2_all_sector_value_count = D2['main_sector'].value_counts()
D2_top_Sector = D2_all_sector_value_count.index[0]
D2_second_best_Sector = D2_all_sector_value_count.index[1]
D2_third_best_Sector = D2_all_sector_value_count.index[2]


print("Top sector (based on count of investments) %s" %(D2_top_Sector))
print("Second best sector (based on count of investments) %s" %(D2_second_best_Sector))
print("Third best sector (based on count of investments) %s" %(D2_all_sector_value_count.index[2]))
print("Number of investments in the top sector : %s " %(D2_all_sector_value_count.iloc[0]))
print("Number of investments in the second best sector : %s " %(D2_all_sector_value_count.iloc[1]))
print("Number of investments in the third best sector : %s " %(D2_all_sector_value_count.iloc[2]))
print("For top sector,company received the highest investment :%s" %(D2[D2["main_sector"] == D2_top_Sector].groupby("name")["raised_amount_usd"].sum().idxmax()))
print("For 2nd best sector,company received the highest investment :%s" %(D2[D2["main_sector"] == D2_second_best_Sector].groupby("name")["raised_amount_usd"].sum().idxmax()))


print("\n############ Country-3 #############\n")
print("Total number of investments %s " %(D3.shape[0]))
print("Total amount of investment %s" %(D3.raised_amount_usd.sum())) 

D3_all_sector_value_count = D3['main_sector'].value_counts()
D3_top_Sector = D3_all_sector_value_count.index[0]
D3_second_best_Sector = D3_all_sector_value_count.index[1]
D3_third_best_Sector = D3_all_sector_value_count.index[2]


print("Top sector (based on count of investments) %s" %(D3_top_Sector))
print("Second best sector (based on count of investments) %s" %(D3_second_best_Sector))
print("Third best sector (based on count of investments) %s" %(D3_all_sector_value_count.index[2]))
print("Number of investments in the top sector : %s " %(D3_all_sector_value_count.iloc[0]))
print("Number of investments in the second best sector : %s " %(D3_all_sector_value_count.iloc[1]))
print("Number of investments in the third best sector : %s " %(D3_all_sector_value_count.iloc[2]))
print("For top sector,company received the highest investment :%s" %(D3[D3["main_sector"] == D3_top_Sector].groupby("name")["raised_amount_usd"].sum().idxmax()))
print("For 2nd best sector,company received the highest investment :%s" %(D3[D3["main_sector"] == D3_second_best_Sector].groupby("name")["raised_amount_usd"].sum().idxmax()))
 

    
    
""" 
Plotting
"""
sns.set()
    

"""
#2nd Plot
"""

formatter = FuncFormatter(billions)
fig, ax = plt.subplots(figsize=(8,10))
ax.yaxis.set_major_formatter(formatter)

br = top_9_countries.plot.bar(rot=0, alpha=1, color='lightblue')
ax.get_children()[0].set_color('darkblue') 
ax.get_children()[2].set_color('darkblue') 
ax.get_children()[2].set_alpha(0.8)
ax.get_children()[3].set_color('darkblue') 
ax.get_children()[3].set_alpha(0.7)
ax.set_ylabel('Total investments in Billion dollor')
ax.set_xlabel('Top 9 Countries')

for index,data in enumerate(top_9_countries):
    plt.text(x=index , y =data+1 , s=f"{ '$%1.1fB' % (data*1e-9)  }" , fontdict=dict(fontsize=9))
plt.show()


"""
#First Plot
"""
    
raw_data = {'Country': ['USA', 'GBR', 'IND'],
        'top_sector': [all_sector_value_count.iloc[0], D2_all_sector_value_count.iloc[0], D3_all_sector_value_count.iloc[0]],
        'second_best_sector': [all_sector_value_count.iloc[1], D2_all_sector_value_count.iloc[1],D3_all_sector_value_count.iloc[1]],
        'third_best_sector': [all_sector_value_count.iloc[2], D2_all_sector_value_count.iloc[2], D3_all_sector_value_count.iloc[2]]}
df = pd.DataFrame(raw_data, columns = ['Country', 'top_sector', 'second_best_sector', 'third_best_sector'])



# Setting the positions and width for the bars
pos = list(range(len(df['top_sector']))) 
width = 0.1
    
# Plotting the bars
fig, ax = plt.subplots(figsize=(8,10))

# Create a bar for top sector data
plt.bar(pos, df['top_sector'], width, alpha=1, color='green', label=df['Country'][0]) 

# Create a bar with second_best_sector data
plt.bar([p + width for p in pos], df['second_best_sector'],width, alpha=1, color='red', label=df['Country'][1]) 

# Create a bar with third_best_sector data
plt.bar([p + width*2 for p in pos], df['third_best_sector'], width, alpha=1, color='blue', label=df['Country'][2]) 


# Set the y axis label
ax.set_ylabel('Number of investments')

# Set the chart's title
ax.set_title('A plot showing the number of investments in the top 3 sectors of the top 3 countries')

tick_spacing = 50
ax.yaxis.set_major_locator(ticker.MultipleLocator(tick_spacing))

# Set the position of the x ticks
ax.set_xticks([p + 0.1 * width for p in pos])

# Set the labels for the x ticks
ax.set_xticklabels(df['Country'])

# Setting the x-axis and y-axis limits
plt.xlim(min(pos)-width, max(pos)+width*4)

# Adding the legend and showing the plot
plt.legend(['top_sector', 'second_best_sector', 'third_best_sector'], loc='upper right')
plt.grid()
plt.show()
         

             
#---------------------- First plot -------------------------
#A plot showing the fraction of total investments (globally) in venture, seed, and private equity, 
#and the average amount of investment in each funding type. This chart should make it clear that a 
#certain funding type (FT) is best suited for Spark Funds.



dd = master_frame[(df_funding == "venture") | (df_funding== "seed") | (df_funding == "private_equity")]

series_total_investments = dd.groupby("funding_round_type")["raised_amount_usd"].sum()
colors = ['cyan', 'yellow', 'lightgreen']
sns.set()
series_total_investments.plot(colors=colors, wedgeprops={'alpha':1}, textprops={'fontsize': 12}, explode=(0,0,0.08),kind='pie', startangle=90, shadow = True,  title='Fraction of total investments', figsize=[8,8],
          autopct=lambda p: '{:.2f}%(${:.0f}B)'.format(p,(p/100)*series_total_investments.sum()*(1e-9)))
plt.show()



