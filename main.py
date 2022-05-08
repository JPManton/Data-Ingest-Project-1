import requests
from requests import get
import pandas as pd
import json
from functions import write_to_blob, exec_stored_proc, load_from_mssql, remove_non_ascii_2
from datetime import date
import os

# Save api details for use on all calls
api_key = 'REDACTED'
headers = {'Authorization': 'Token {}'.format(api_key)}

# Generate the start and end date variables based on max date of the SQL tables
start_date_load = load_from_mssql("select maxDate from playpen.accuranker_max_dates")
start_date = start_date_load["maxDate"].max()
end_date = date.today()

print("Stage 1 Complete - Imports and variables set")

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Simple list of our domains
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# Run the api and generate the link
url = 'http://app.accuranker.com/api/v4/domains/'
params = {
    'fields': 'id,domain,display_name'
}

# Save the json data in the variable r and then put that json into a dataframe for storage
r = requests.get(url, headers=headers, params=params)
allDomains = pd.DataFrame(r.json())

# Save two versions of the file, one to put into the inpur blob folders and one to put in historic table
allDomains.to_csv("allDomains.csv", sep='|', index=False)
allDomains.to_csv("allDomains_" + str(start_date) + "__" + str(end_date) + ".csv", sep='|', index=False)

# Write the current days file to blob
write_to_blob(path_to_file="./allDomains.csv", blob_folder="Accuranker/allDomains",
              container="playpen", storage_account="005")

# Write the historic file into blob with start and end date in the title
write_to_blob(path_to_file="./allDomains_" + str(start_date) + "__" + str(end_date) + ".csv",
              blob_folder="Accuranker/Historic_data", container="playpen", storage_account="005")

# delete both files from the folder
os.remove("./allDomains.csv")
os.remove("./allDomains_" + str(start_date) + "__" + str(end_date) + ".csv")

print("Stage 2 Complete - domain lookup table")

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Domains with historic details
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# Run the api and generate the link
url = 'http://app.accuranker.com/api/v4/domains/'
params = {
    'fields': 'id,domain,display_name,history.date,history.ranking_movement,history.ranking_distribution,history.share_of_voice,history.share_of_voice_percentage,history.average_rank',
    'period_from': start_date,
    'period_to': end_date
}

# Save the json data in the variable r and then put that json into a dataframe for storage
r = requests.get(url, headers=headers, params=params)
response = json.loads(r.text)
allDomainsHist = pd.DataFrame(r.json())

# empty list ready for columns to add to the new dataframe
listoflists = []

for item in response:
    histobjects = item["history"]
    id_col = item["id"]
    domain_col = item["domain"]
    display_col = item["display_name"]
    for histitem in histobjects:
        date_col = histitem["date"]
        sov = histitem["share_of_voice"]
        sovPer = histitem["share_of_voice_percentage"]
        avgrnk = histitem["average_rank"]
        rank_move_items = histitem["ranking_movement"]
        for rank_mov_item in rank_move_items:
            winners_col = rank_move_items["winners"]
            win_sov_col = rank_move_items["share_of_voice_winners"]
            losers_col = rank_move_items["losers"]
            losers_sov_col = rank_move_items["share_of_voice_losers"]
            nomov_col = rank_move_items["no_movement"]
            nomov_sov_col = rank_move_items["share_of_voice_no_movement"]
        rank_dist_items = histitem["ranking_distribution"]
        for rank_dist_item in rank_dist_items:
            nought3 = rank_dist_items["keywords_0_3"]
            nought10 = rank_dist_items["keywords_4_10"]
            nought20 = rank_dist_items["keywords_11_20"]
            nought50 = rank_dist_items["keywords_21_50"]
            nought50Plus = rank_dist_items["keywords_above_50"]
            unranked = rank_dist_items["keywords_unranked"]
            total = rank_dist_items["keywords_total"]
            # print(histitem)
            newRow = [
                id_col, domain_col, display_col, date_col, sov, sovPer, avgrnk, winners_col, win_sov_col, losers_col,
                losers_sov_col, nomov_col, nomov_sov_col, nought3, nought10, nought20, nought50,
                nought50Plus, unranked, total
            ]
        # print(newRow)
        listoflists.append(newRow)

# generate column headers for the new dataframe of data
columnHeaders = ["id", "domain", "date", "display_name", "shareofvoice", "shareofvoicepercentage", "averagerank",
                 "winners",
                 "winners_shareofvoice", "losers", "losers_shareofvoice", "nomovement", "nomovement_shareofvoice",
                 "keywords_0_3", "keywords_4_10", "keywords_11_20", "keywords_21_50", "keywords_above_50",
                 "keywords_unranked", "keywords_total"]
allDomainsHist = pd.DataFrame(listoflists, columns=columnHeaders)

# Save two versions of the file, one to put into the inpur blob folders and one to put in historic table
allDomainsHist.to_csv("allDomainsHist.csv", sep='|', index=False)
allDomainsHist.to_csv("allDomainsHist_" + str(start_date) + "__" + str(end_date) + ".csv", sep='|', index=False)

# Write the current days file to blob
write_to_blob(path_to_file="./allDomainsHist.csv", blob_folder="Accuranker/domainHistoric",
              container="playpen", storage_account="005")

# Write the historic file into blob with start and end date in the title
write_to_blob(path_to_file="./allDomainsHist_" + str(start_date) + "__" + str(end_date) + ".csv",
              blob_folder="Accuranker/Historic_data", container="playpen", storage_account="005")

# delete both files from the folder
os.remove("./allDomainsHist.csv")
os.remove("./allDomainsHist_" + str(start_date) + "__" + str(end_date) + ".csv")

print("Stage 3 Complete - domain historic table")

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Simple list of our tags
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# empty list ready for columns to add to the new dataframe
domainCodes = list(allDomains["id"])
listoflists = []

for item in domainCodes:

    # Run the api and generate the link
    url = 'https://app.accuranker.com/api/v4/domains/{}/tags/'.format(item)
    params = {
        'fields': 'tag, history',
        'period_from': start_date,
        'period_to': end_date
    }

    # Save the json data in the variable r and then put that json into a dataframe for storage
    r = requests.get(url, headers=headers, params=params)
    response = json.loads(r.text)

    # Domain id ready to loop through and use as a column header
    domainname = list(allDomains["id"][allDomains["id"] == item])

    for item in response:
        histobjects = item["history"]
        tag = item["tag"]
        for histitem in histobjects:
            # print(histitem)
            newRow = [
                str(domainname),
                tag,
                histitem["date"],
                histitem["average_rank"],
                histitem["share_of_voice"],
                histitem["share_of_voice_percentage"],
                histitem["search_volume"],
                histitem["keywords"],
            ]
            # print(newRow)
            listoflists.append(newRow)

# generate column headers for the new dataframe of data
columnHeaders = ["domainname", "tag", "date", "average_rank", "share_of_voice", "share_of_voice_percentage",
                 "search_volume", "keywords"]
allTags = pd.DataFrame(listoflists, columns=columnHeaders)

# Save two versions of the file, one to put into the inpur blob folders and one to put in historic table
allTags.to_csv("allTags.csv", sep='|', index=False)
allTags.to_csv("allTags_" + str(start_date) + "__" + str(end_date) + ".csv", sep='|', index=False)

# Write the current days file to blob
write_to_blob(path_to_file="./allTags.csv", blob_folder="Accuranker/allTags",
              container="playpen", storage_account="005")

# Write the historic file into blob with start and end date in the title
write_to_blob(path_to_file="./allTags_" + str(start_date) + "__" + str(end_date) + ".csv",
              blob_folder="Accuranker/Historic_data", container="playpen", storage_account="005")

# delete both files from the folder
os.remove("./allTags.csv")
os.remove("./allTags_" + str(start_date) + "__" + str(end_date) + ".csv")

print("Stage 4 Complete - tags")

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Simple list of our landing pages
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# empty list ready for columns to add to the new dataframe
domainCodes = list(allDomains["id"])
listoflists = []

for item in domainCodes:

    # Run the api and generate the link
    url = 'https://app.accuranker.com/api/v4/domains/{}/landing_pages/?fields=id,path,created_at,history'.format(item)
    params = {
        'period_from': start_date,
        'period_to': end_date
    }

    # Save the json data in the variable r and then put that json into a dataframe for storage
    r = requests.get(url, headers=headers, params=params)
    response = json.loads(r.text)

    # Domain id ready to loop through and use as a column header
    domainname = list(allDomains["id"][allDomains["id"] == item])

    for item in response:
        histobjects = item["history"]
        id_column = item["id"]
        path_column = item["path"]
        created_at = item["created_at"]
        for histitem in histobjects:
            # print(histitem)
            newRow = [
                str(domainname),
                id_column,
                path_column,
                created_at,
                histitem["date"],
                histitem["average_rank"],
                histitem["share_of_voice"],
                histitem["share_of_voice_percentage"],
                histitem["search_volume"],
                histitem["keywords"],
            ]
            # print(newRow)
            listoflists.append(newRow)

# generate column headers for the new dataframe of data
columnHeaders = ["domainname", "id", "path", "created_at", "date", "average_rank", "share_of_voice",
                 "share_of_voice_percentage", "search_volume", "keywords"]
all_landing_pages = pd.DataFrame(listoflists, columns=columnHeaders)

# Save two versions of the file, one to put into the inpur blob folders and one to put in historic table
all_landing_pages.to_csv("all_landing_pages.csv", sep='|', index=False)
all_landing_pages.to_csv("all_landing_pages_" + str(start_date) + "__" + str(end_date) + ".csv", sep='|', index=False)

# Write the current days file to blob
write_to_blob(path_to_file="./all_landing_pages.csv", blob_folder="Accuranker/allLandingPages",
              container="playpen", storage_account="005")

# Write the historic file into blob with start and end date in the title
write_to_blob(path_to_file="./all_landing_pages_" + str(start_date) + "__" + str(end_date) + ".csv",
              blob_folder="Accuranker/Historic_data", container="playpen", storage_account="005")

# delete both files from the folder
os.remove("./all_landing_pages.csv")
os.remove("./all_landing_pages_" + str(start_date) + "__" + str(end_date) + ".csv")

print("Stage 5 Complete - landing pages")

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Simple list of our keywords
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# empty list ready for columns to add to the new dataframe#empty list ready for columns to add to the new dataframe
domainCodes = list(allDomains["id"])
listoflists = []

for item in domainCodes:

    # Run the api and generate the link
    url = 'http://app.accuranker.com/api/v4/domains/{}/keywords/?fields=id,created_at,keyword,tags,search_volume,search_type,ranks'.format(
        item)
    params = {
        'period_from': start_date,
        'period_to': end_date
    }

    # Save the json data in the variable r and then put that json into a dataframe for storage
    r = requests.get(url, headers=headers, params=params)
    response = json.loads(r.text)

    # Domain id ready to loop through and use as a column header
    domainname = list(allDomains["id"][allDomains["id"] == item])

    for item in response:
        idov_col = item["id"]
        key_col = item["keyword"]
        cra_col = item["created_at"]
        tag_col = item["tags"]
        search_t_col = item["search_type"]
        search_volume_col = item["search_volume"]
        rankobjects = item["ranks"]
        for rankitem in rankobjects:
            rnkid_col = rankitem["id"]
            ca_col = rankitem["created_at"]
            rank_col = rankitem["rank"]
            sov_col = rankitem["share_of_voice"]
            sovp_col = rankitem["share_of_voice_percentage"]
            hrp_col = rankitem["highest_ranking_page"]
            if "is_featured_snippet" not in response:
                fn_col = "None"
            else:
                fn_col = rankitem["is_featured_snippet"]
            if "has_video" not in response:
                vid_col = "None"
            else:
                vid_col = rankitem["has_video"]
            if rankitem["landing_page"] is None:
                path_col = "No Path"
            else:
                path_col = rankitem["landing_page"]["id"]
            if rankitem["title_description"] is None:
                title_col = "No Title"
            else:
                title_col = rankitem["title_description"]["title"]
            # Remove any non ascii charaters from the title description
            title_col = remove_non_ascii_2(title_col).replace("|", " ")
            # generate row to add into the list of lists
            newRow = [
                str(domainname), idov_col, key_col, cra_col, tag_col, search_t_col, search_volume_col,
                rnkid_col, ca_col, rank_col, sov_col, sovp_col, hrp_col, fn_col, vid_col, path_col,
                title_col
            ]
            listoflists.append(newRow)

# generate column headers for the new dataframe of data
columnHeaders = ["domainname", "keyword_id", "keyword", "created_at", "tags", "search_type", "search_volume",
                 "rank_id", "rank_created_at", "rank", "share_of_voice", "share_of_voice_percentage",
                 "highest_rank_page", "is_featured_snippet", "has_video", "landing_page", "title_description"]
allKeywords = pd.DataFrame(listoflists, columns=columnHeaders)

# Save two versions of the file, one to put into the inpur blob folders and one to put in historic table
allKeywords.to_csv("allKeywords.csv", sep='|', index=False)
allKeywords.to_csv("allKeywords_" + str(start_date) + "__" + str(end_date) + ".csv", sep='|', index=False)

# Write the current days file to blob
write_to_blob(path_to_file="./allKeywords.csv", blob_folder="Accuranker/allKeywords",
              container="playpen", storage_account="005")

# Write the historic file into blob with start and end date in the title
write_to_blob(path_to_file="./allKeywords_" + str(start_date) + "__" + str(end_date) + ".csv",
              blob_folder="Accuranker/Historic_data", container="playpen", storage_account="005")

# delete both files from the folder
os.remove("./allKeywords.csv")
os.remove("./allKeywords_" + str(start_date) + "__" + str(end_date) + ".csv")

print("Stage 6 Complete - keywords base table")

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Simple list of our competitors keywords
# To join to our other keywords
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# empty list ready for columns to add to the new dataframe
domainCodes = list(allDomains["id"])
listoflists = []

for item in domainCodes:

    # Run the api and generate the link
    url = 'http://app.accuranker.com/api/v4/domains/{}/keywords/?fields=id,created_at,keyword,competitor_ranks'.format(
        item)
    params = {
        'period_from': start_date,
        'period_to': end_date
    }

    # Save the json data in the variable r and then put that json into a dataframe for storage
    r = requests.get(url, headers=headers, params=params)
    response = json.loads(r.text)

    # Domain id ready to loop through and use as a column header
    domainname = list(allDomains["id"][allDomains["id"] == item])

    for item in response:
        idov_col = item["id"]
        key_col = item["keyword"]
        cra_col = item["created_at"]
        rankobjects = item["competitor_ranks"]
        for rankitem in rankobjects:
            comprnkid_col = rankitem["id"]
            compca_col = rankitem["created_at"]
            comprank_col = rankitem["rank"]
            compsov_col = rankitem["share_of_voice"]
            compsovp_col = rankitem["share_of_voice_percentage"]
            comphrp_col = rankitem["highest_ranking_page"]
            comp_id_col = rankitem["competitor"]["id"]
            comp_domain_col = rankitem["competitor"]["domain"]
            comp_display_col = rankitem["competitor"]["display_name"]
            # generate row to add into the list of lists
            newRow = [
                str(domainname), idov_col, key_col, cra_col, comprnkid_col, compca_col, comprank_col,
                compsov_col, compsovp_col, comphrp_col, comp_id_col, comp_domain_col, comp_display_col
            ]
            listoflists.append(newRow)

# generate column headers for the new dataframe of data
columnHeaders = ["domainname", "keyword_id", "keyword", "created_at", "ID", "rank_created_at", "rank",
                 "share_of_voice", "share_of_voice_percentage", "highest_rank_page", "competitor_id",
                 "competitor", "competitor_display_name"
                 ]

allKeywordsComp = pd.DataFrame(listoflists, columns=columnHeaders)

# Save two versions of the file, one to put into the inpur blob folders and one to put in historic table
allKeywordsComp.to_csv("allKeywordsCompetitors.csv", sep='|', index=False)
allKeywordsComp.to_csv("allKeywordsCompetitors_" + str(start_date) + "__" + str(end_date) + ".csv", sep='|', index=False)

# Write the current days file to blob
write_to_blob(path_to_file="./allKeywordsCompetitors.csv", blob_folder="Accuranker/allKeywordsCompetitors",
              container="playpen", storage_account="005")

# Write the historic file into blob with start and end date in the title
write_to_blob(path_to_file="./allKeywordsCompetitors_" + str(start_date) + "__" + str(end_date) + ".csv",
              blob_folder="Accuranker/Historic_data", container="playpen", storage_account="005")

# delete both files from the folder
os.remove("./allKeywordsCompetitors.csv")
os.remove("./allKeywordsCompetitors_" + str(start_date) + "__" + str(end_date) + ".csv")

print("Stage 7 Complete - Competitors keywords base table")

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Simple list of our keyword search volume
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# empty list ready for columns to add to the new dataframe
domainCodes = list(allDomains["id"])
listoflists = []

for item in domainCodes:

    # Run the api and generate the link
    url = 'http://app.accuranker.com/api/v4/domains/{}/keywords/?fields=id,keyword,created_at,search_volume.search_volume'.format(
        item)
    params = {
        'period_from': start_date,
        'period_to': end_date
    }

    # Save the json data in the variable r and then put that json into a dataframe for storage
    r = requests.get(url, headers=headers, params=params)
    response = json.loads(r.text)

    # Domain id ready to loop through and use as a column header
    domainname = list(allDomains["id"][allDomains["id"] == item])

    for item in response:
        id_col = item["id"]
        keyword_col = item["keyword"]
        crdate = item["created_at"]
        searchvolume_col = item["search_volume"]["search_volume"]
        newRow = [
            str(domainname),
            id_col,
            keyword_col,
            crdate,
            searchvolume_col,
        ]
        # print(newRow)
        listoflists.append(newRow)

# generate column headers for the new dataframe of data
columnHeaders = ["domainname", "keyword_id", "keyword", "created_at", "search_volume"]
allKeywordsSearchVolume = pd.DataFrame(listoflists, columns=columnHeaders)

# Save two versions of the file, one to put into the inpur blob folders and one to put in historic table
allKeywordsSearchVolume.to_csv("allKeywordsSearchVolume.csv", sep='|', index=False)
allKeywordsSearchVolume.to_csv("allKeywordsSearchVolume_" + str(start_date) + "__" + str(end_date) + ".csv"
                               , sep='|', index=False)

# Write the current days file to blob
write_to_blob(path_to_file="./allKeywordsSearchVolume.csv", blob_folder="Accuranker/allKeywordsSearchVolume",
              container="playpen", storage_account="005")

# Write the historic file into blob with start and end date in the title
write_to_blob(path_to_file="./allKeywordsSearchVolume_" + str(start_date) + "__" + str(end_date) + ".csv",
              blob_folder="Accuranker/Historic_data", container="playpen", storage_account="005")

# delete both files from the folder
os.remove("./allKeywordsSearchVolume.csv")
os.remove("./allKeywordsSearchVolume_" + str(start_date) + "__" + str(end_date) + ".csv")

print("Stage 8 Complete - Search volume keywords base table")

# -------------------------------------------------------------------------------------------------------------------------------------------------------------
# Simple list of our keywords and the tags
# -------------------------------------------------------------------------------------------------------------------------------------------------------------

# empty list ready for columns to add to the new dataframe
domainCodes = list(allDomains["id"])
listoflists = []

for item in domainCodes:

    # Run the api and generate the link
    url = 'http://app.accuranker.com/api/v4/domains/{}/keywords/?fields=id,keyword,tags'.format(
        item)
    params = {
        'period_from': start_date,
        'period_to': end_date
    }

    # Save the json data in the variable r and then put that json into a dataframe for storage
    r = requests.get(url, headers=headers, params=params)
    response = json.loads(r.text)

    # Domain id ready to loop through and use as a column header
    domainname = list(allDomains["id"][allDomains["id"] == item])

    for item in response:
        idov_col = item["id"]
        key_col = item["keyword"]
        tag_object = item["tags"]
        if tag_object is None:
            tag_col = 'None'
        else:
            for item in tag_object:
                tag_col = item
                # generate row to add into the list of lists
                newRow = [
                    str(domainname), idov_col, key_col, tag_col
                ]
                listoflists.append(newRow)

# generate column headers for the new dataframe of data
columnHeaders = ["domainname", "keyword_id", "keyword", "tags"]
allKeywords_tags = pd.DataFrame(listoflists, columns=columnHeaders)

# Save two versions of the file, one to put into the inpur blob folders and one to put in historic table
allKeywords_tags.to_csv("allKeywords_tags.csv", sep='|', index=False)
allKeywords_tags.to_csv("allKeywords_tags_" + str(start_date) + "__" + str(end_date) + ".csv", sep='|', index=False)

# Write the current days file to blob
write_to_blob(path_to_file="./allKeywords_tags.csv", blob_folder="Accuranker/allKeywords_tags",
              container="playpen", storage_account="005")

# Write the historic file into blob with start and end date in the title
write_to_blob(path_to_file="./allKeywords_tags_" + str(start_date) + "__" + str(end_date) + ".csv",
              blob_folder="Accuranker/Historic_data", container="playpen", storage_account="005")

# delete both files from the folder
os.remove("./allKeywords_tags.csv")
os.remove("./allKeywords_tags_" + str(start_date) + "__" + str(end_date) + ".csv")

print("Stage 9 Complete - Keywords and tag combos")
