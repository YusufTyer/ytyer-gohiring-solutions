
# GOhiring Data Team Code Challenge

## Setup
1. Please clone this repo to your local machine. When submitting solution files, please create a new public repository. (Forking would lead to solutions to this code challenge being publicly available and easy to find).
2. In this repository, there are three files `jobs.json`, `companies.json`, and `locations.csv`. You will need to load these files, join them on the relevant fields, and answer the questions below.
You can use any tool or combination of tools with which you are familiar, however Python/Jupyter, Power BI, and SQL are most relevant to this position.

## Questions
- Be prepared to discuss:
  - How the data was loaded.
  - Data quality issues found and how they were discovered.
  - How you found answers to the questions below (and any caveats to the answers). 

1. What location has the most jobs that are either posted or expired?
2. What month had the most cancelled jobs?
3. Which company has the highest ratio of posted jobs to employee count?
4. Develop the SQL to define a dimensional model schema for this data. Document (or be ready to discuss) any design decisions that you make.
5. Using your dimensional model, write a SQL query that returns a list of jobs for each company, ordered and enumerated within each group by the `posted_at` date.
6. Discuss how you would obtain and model information (within your schema) about the duration of jobs (from `posted` to `expired` states).
7. (For Data Analyst Role) Provide a visualization showing the trend of the number of active jobs over time. Also include the company name data in this visualization.

## Submission
Please submit a file with the answers to the above questions, as well as any other files used to obtain the solutions, via the new Github repository that you created.  Provide the link/name of the repo in your email response. 
Note: If binary files are generated that don't make sense to submit through Github, please send those files along with the email response as well (please keep this under 10MB.)
