---
layout: page
---

# 6113 Assignments

The following are steps to setup, complete, and submit your assignments

1. Get started by forking the `databass-public` repository into the private repo `databass-[YOUR UNI]` under your github account. You can request unlimited private repos as a student by getting the [student pack from GitHub](https://education.github.com/pack). Below is the recommended steps for forking a public repo into private repo:

```bash
# 1) Create a bare clone of the repo locally (this temporary bare clone will be removed in the future)
git clone --bare git@github.com:w6113/databass-public.git

# 2) Create a new empty private repository on GitHub.com and name it databass-[YOUR UNI]. 

# 3) Mirror-push your local bare clone into the databass-[YOUR UNI]
cd databass-public.git
git push --mirror git@github.com:[YOUR GITHUB USERNAME]/databass-[YOUR UNI].git

# 4) Remove the temporary local bare repo from step 1
cd ..
rm -rf databass-public.git

# 5) Clone the now privately remote databass repository into your local workspace
cd [YOUR WORKING DIRECTORY]
git clone git@github.com:[YOUR GITHUB USERNAME]/databass-[YOUR UNI].git

# 6) Add the original databass-public repo into your git remote config for potential future changes, and make sure to disable push on the remote repo
git remote add upstream git@github.com:w6113/databass-public.git
git remote set-url --push upstream DISABLE 
```

If you follow step 1-6, you can check if your remote repo configuration is set up properly by running `git remote -v`. This command should output the following:

```bash
origin	git@github.com:[YOUR GITHUB USERNAME]/databass-[YOUR UNI].git (fetch)
origin	git@github.com:[YOUR GITHUB USERNAME]/databass-[YOUR UNI].git (push)
upstream	git@github.com:w6113/databass-public.git (fetch)
upstream	DISABLE (push)
```

To push changes for your assignment, you should use `origin`, eg: `git push origin [branch]`.

If you want to update changes from `databass-public` to your branch, you can just fetch the remote `upstream` and rebase/merge it on top of your work. You can run the followingn command for updating your work with the upstream changes:

```
  git fetch upstream
  git rebase upstream/master
```

and resolve conflicts if any.

2. Make sure that your repository is PRIVATE and named `databass-UNI`.
3. Add the staff's github accounts to the repo: `deka108` and `sirrice`
4. For each assignment, create a branch named "aX" where "X" is the specific assignment.  For instance, the branch for the 0th assignment will be "a0" (all lower case).
5. Commit and push your solution to github.  For instance, the following for the "a0" branch:

```
      git push --set-upstream origin a0
```

6. Once the deadline has passed, the staff's scripts will automatically pull the code from the assignment's branch and run our private test cases.  


Each assignment includes a set of basic test cases to help you sanity check your solution.   **Be aware that the tests are woefully incomplete**. Thus, we strongly encourage you that come up with, and write you own test cases to more thoroughly evaluate your own code.    We will assess your solutions using those included in the assignment, alongside a private set of test cases.

## Asking Questions

You can ask questions in the `#assignments` channel on slack.

## Reporting Bugs

You can report bugs on databass in the `#assignments` channel on slack and optionally open up a new GitHub issue on the `databass-public` repo. Before creating a new GitHub issue, please check first with the previous issues on the repo in case the bugs that you discover had already been reported / resolved before.

<!-- ## Submission Overview

Need to decide on how to submit if the above procedure is hard to automate

* Use this pytest autograder?
  * https://github.com/ucsb-gradescope-tools/sample-python-pytest-autograder
  * https://github.com/ucsb-gradescope-tools/pytest_utils
* Follow Chicago's protocol? https://github.com/UCHI-DB/course-info#submitting-your-lab -->


## Assignments

* [A0](./a0.md): Warmup assignment
* [A1](./a1.md): Implement iterator operator logic
* [A2](./a2.md): Join optimization
* [A3](./a3.md): Query Compilation
* [A4](./a4.md): Benchmarking
* [A5](./a5.md): Lineage (optional)
