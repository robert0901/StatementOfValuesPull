# StatementOfValuesPull
Pulling statement of values for Property and Auto Members we cover.

Problem: Prior to creating these two scripts, our team would manually go to the Kroll/Duff and Phelps webpage download the XLS files, convert the values to Excel Workbooks then run it through an Access Database for validation and cleaning with us having to manually update dates and run reports.

Solution: I proposed that we create a dynamic scrape that would download and read the orginal XLS files into a pandas dataframe then do the validation, cleaning, outputting error report, and uploading to a specificed directory in a seperate script. This new process saves our team time and it much more effiecent process. The data team takes the prepped files and upload to our datawarehouse using SQL Alchemy. In the future it may be useful to run the process in synchronized with the data upload into the data warehouse. For now, we still view the error report before moving forward with the second phase.

Knowwledge Learned: I had done similar scrapes with my previous job using Python's Selenium to dyanimically download CSV files and concanate them. For this project, I used Python's Playwright which I found to be very user friendly. I revisted Class programming that I had previously learned in my last job. This helped keep processes seperate while building this tool. This was the first time I also worked with XLS file having no knowledge that they can be read using an HTML parser such pd.read_html().
