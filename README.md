# algo_trading
algo trading application

# Set Up Instructions

First, you must create a [github access token](https://docs.github.com/en/github/authenticating-to-github/keeping-your-account-and-data-secure/creating-a-personal-access-token#creating-a-token).

Then, clone this repo by running the following command in the terminal:
```
cd {directory/to/store/repo}
git clone https://github.com/ryanlattanzi/algo_trading
```
When prompted for username and password, paste the access token instead of the password. ([like here](https://docs.github.com/en/github/authenticating-to-github/keeping-your-account-and-data-secure/creating-a-personal-access-token#using-a-token-on-the-command-line))

To start the local environment, run the command
```
docker-compose -f docker-compose-setup-db.yml
```

Now, Postgres should be running on port `5432` with name `algo_trading_postgres_1`, and PGAdmin should be running on port `80` with name `algo_trading_pgadmin_1`.

# Useful Git Commands

- Typical workflow for setting up a new branch:
```
git checkout master
git pull
git checkout -b {new/branch/name}
```

- After doing your work for the branch, we need to push it to the remote repo and create a pull request:
```
git checkout {branch/name}
git add -A
git commit -m 'commit message to describe changes'
git push

[git push might ask you to --set-upstream in order to
push your local branch to the remote branch]
```

- Now, in the github console, you should see an indicator that you recently pushed your branch to the repo. It will give an option for you to create a pull request, which you should do and request review from the other hitta.
