import pandas as pd
import re
from bs4 import BeautifulSoup

df = pd.read_csv("dataset project.csv")

print(df.shape[0])

list = []
cnt = 0
for i in df.values:
    splits = str(i).split("\\n")
    # values.split("\n")
    for j in splits:
        if len(j.split()) > 1:
            list.append(BeautifulSoup(j, 'html.parser').get_text())
        # df2.iloc(cnt) = j
        # cnt+=1

df2 = pd.DataFrame({'body': list})
df2.to_csv("data2.csv")
print(df2.head())

#
# @udf
# def getIssueType(IssueDescription):
#   # returns ‘b’ for bug, ‘f’ for new feature and ‘e’ for feature enhancement.
#
# @udf
# def getYear(CreationTime):
#   # return Creation year
#
# @udf
# def getMonth(CreationTime):
#   # return Creation month
#
# @udf
# def getDay(CreationTime):
#   # return Creation day
#
# df2 = df.withColumn("IssueYear", getYear(df.CreationTime))
# df3 = df2.withColumn("IssueMonth", getMonth(df.CreationTime))
# df4 = df3.withColumn("IssueDay", getDay(df.CreationTime))
# df5 = df4.withColumn("IssueType", getIssueType(df.IssueDescription))
# df5.show()    #See the results after adding the above columns
#
# print(df5.agg({"NumberOfComponentsAffected":"sum"}).collect()[0][0])
#
# from pyspark.sql.functions import *
# df.select(avg("IssueSeverity")).show()
#
# df5.groupby("IssueType").count().show()
#
# df5.groupby("IssueYear").count().show()
#
# map(key, value):
#     # key: IPs; value: Size_of_data_transferred
#     emit(tuple([getCountry(key), getCity(key)]), value)
#
# getCountry(IP):
#     # returns the country as a string
#
# getCity(IP):
#     # returns the city as a string
#
# reduce(key, values):
#     # key: a tuple of ("Country", "City"); value: an iterator over amount of data consumed
#     result = 0
#     for v in values:
#         result += v
#     emit(key, result)
#
# combiner(data):
#     # data is the whole pairs of <key, value> coming from the mapper
#     # result = an empty hash map
#     # for each pair in the data:
#     #     the element in the result having the same key as the key of pair:
#     #       update the value with the same key in result by appending the existing value with value of the pair
#     # return result
#
# @udf(returnType=IntegerType())
# def count_friends(friends_id_list):
#   return len(friends_id_list)
#
# df2 = df.withColumn("NumberOfFriends", count_friends(df.Friend_profile_id_list))
# df2.show()
#
# import pyspark.sql.functions as f
# df2.filter((f.col("Profile_id")== wanted_profile_id)).show()
# print(df2.filter((f.col("Profile_id")== wanted_profile_id)).select("NumberOfFriends").collect()[0][0])
#
# # l1 = friends list of profile_id1
# # l2 = friends list of profile_id2
#
# def find_friends(friend_list_1, friend_list_2):
#     common_friends = []
#     for friend_id in friend_list_1:
#         if friend_id in friend_list_2:
#             common_friends.append(friend_id)
#
#     return common_friends
#
# print(find_friends(l1, l2))
#
# map(key, value):
#     # key: Profile_id; value: list of friends
#     for friend in values:
#         emit(key, 1)
#
# reduce(key, values):
#     # key: Profile_id; value: list of 1s, each from a friend in the initial friend list
#     result = 0
#     for v in values:
#         result += v
#     emit(key, result)
#
#
import pyspark.sql.functions as f

print(
    dataFrame.where(dataFrame.HasAcceptedAnswer == "True"). \
        groupBy('HasAcceptedAnswer').agg(f.sum("Score")).collect()[0][1])


n_false_questions = df.where(dataFrame.HasAcceptedAnswer == "False").\
groupBy('HasAcceptedAnswer').agg(F.sum("Score")).collect()[0][1]

n_true_questions = df.where(dataFrame.HasAcceptedAnswer == "True").\
groupBy('HasAcceptedAnswer').agg(F.sum("Score")).collect()[0][1]

print(n_true_questions - n_false_questions)