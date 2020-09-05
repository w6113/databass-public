---
layout: page
---

# 6113 Assignments

The following are steps to setup, complete, and submit your assignments

1. Get started by forking the databass repository into your own github account.
2. Make sure that your repository is PRIVATE
3. Add the staff's github accounts to the repo: lamflokas and sirrice
4. For each assignment, create a branch named "aX" where "X" is the specific assignment.  For instance, the branch for the 0th assignment will be "a0" (all lower case)
5. Commit and push your solution to github.  For instance, the following for the "a0" branch:

      git push --set-upstream origin a0

5. Once the deadline has passed, the staff's scripts will automatically pull the code from the assignment's branch and run our private test cases.  


Each assignment includes a set of basic test cases to help you sanity check your solution.   **Be aware that the tests are woefully incomplete**. Thus, we strongly encourage you that come up with, and write you own test cases to more thoroughly evaluate your own code.    We will assess your solutions using those included in the assignment, alongside a private set of test cases.

## Submission Overview

Need to decide on how to submit if the above procedure is hard to automate

* Use this pytest autograder?
  * https://github.com/ucsb-gradescope-tools/sample-python-pytest-autograder
  * https://github.com/ucsb-gradescope-tools/pytest_utils
* Follow Chicago's protocol? https://github.com/UCHI-DB/course-info#submitting-your-lab


## Assignments

* [A0](./a0): Warmup assignment
* [A1](./a1): Implement iterator operator logic
* [A2](./a2): Join optimization
* [A3](./a3): Query Compilation
