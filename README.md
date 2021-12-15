# cmpt_732
Project for CMPT 732 Big Data Lab 1

## Contributors
[Kaumil Trivedi](https://github.com/kaumil)
[Gurashish Singh Suri](https://github.com/Gurashish)
[Arshdeep Singh Ahuja](https://github.com/arshahuja)
[Gabriel Xu Henderson](https://github.com/Gabriel737)

## Frontend URL: www.cadorsmap.ca
### Account name: cadorsquicksightuser
### Username: quicksight_reader
### Password: quicksight@cadorscmpt732


# Brief Overview:
* This project is first implemented locally/Google Collab(for the spark ETL). Later, it is modified to work as per the stipulated pipeline on AWS.
* The code for the two implementations is a bit different. The code for the local part is done under api/ and for the AWS section it is done under aws/.

# Scraping instructions

* Step 1: Make sure we have VS Code installed.
* Step 2: Change directory to the folder you want project to be cloned using cd in terminal of VS Code.
* Step 3: Clone repository using: git clone git@csil-git1.cs.surrey.sfu.ca:knt7/cmpt_732.git
* Step 4: pip install all the libraries in requirements.txt using: 
	* python3 -m pip install -r requirements.txt
* Step 5: Make sure you see all the files in file explorer (top left in VS Code)
* Step 6: In config.ini set url_scrape_start and url_scrape_limit ( currently set up 21 and 30 ) to set page numbers as following:
	*	 Arshdeep: 1-5000 
	*	 Gurashish: 5001-10000
	*	 Gabriel: 10001-15000
	*	 Kaumil: 15001- 20336
* Step 7. While running go to current directory and observe occurances_output folder to see if we have .txt files with 75 urls in each of them. Terminal will display page being scrapped
* Step 8. After successful run we will get 1000 .txt files in this folder.
* Step 9: Now in main.py comment code from 12 - 22 and uncomment all the code previously commented in Step 7. 
* Step 10: Run main.py to scan all url's from occurances_output and get detailed data for each one of them.
* Step 11: Observe page_data_output folder where we will have 1 file for each file in occurances_output having detailed data as json. Terminal will display number of url’s scrapped
* Step 12: After code runs make sure we have 1000 json's in page_data_output folder and hence data for 75000 cador incidents.

