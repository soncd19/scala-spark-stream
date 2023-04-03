# firstproject
NOTE
* Please create your brand by following:
	1. git checkout -b your_new_branch
	2. Create it: git add. --> git commit --amend -m "yourcommentfornewbranch" --> git push origin your_new_branch -f
* When you wanna update something new on your branch:
	1. git add . 
	2. git commit --amend -m "yourcomment_with_version"
	3. git push origin your_branch -f
* When you wanna upgrade your branch from master:
	1. git checkout master
	2. git pull origin master
	3. git checkout your_branch
	4. git rebase master
