import click
import subprocess
import shutil

assignments = ['A%s' % i for i in range(0,4)]
digits = "1234567890"
check_bad_uni = lambda uni: uni is None or any(c not in digits for c in uni[-4:])

@click.command()
@click.option("-u1", 
  prompt="Your UNI in the format of AA1234", 
  help="UNI in AA1234 format")
@click.option("-u2",
  prompt="Your teammate's UNI or the word 'NONE' if you don't have a teammate",
  help="Teammate's UNI, or 'NONE' if no teammate")
@click.option('-a', prompt="The assignment you are submitting", type=click.Choice(assignments))
@click.option('-q', is_flag=True, help="Submit without prompting")
def main(u1, u2, a, q):
  """
  Script to package up your DataBass submission.  You should run this in a UNIX-based environment.
  """
  uni1 = u1
  uni2 = u2
  assignment = a

  if check_bad_uni(uni1):
    print("Your UNI should be in the format of AA1234.")
    print("You submitted: %s" % uni1)
    return 

  if check_bad_uni(uni2):
    if uni2 == 'NONE':
      pass
    else:
      print("Your teammate's UNI should be in the format of AA1234 or 'NONE' if you are working alone.")
      print("You submitted: %s" % uni2)
      return

  if assignment == None:
    print("Choose an assignment.  Use --help to see options")
    return

  if not q:
    cmd = raw_input("You submitted %s, %s, and %s.  Type Y to submit: " % (uni1, uni2, assignment))
    if cmd.lower() != "y":
      return

  # Package and check the code
  fname = "%s_%s_%s" % (assignment, uni1, uni2)
  shutil.make_archive(fname, 'zip', "databass", ".")


if __name__ == "__main__":
  main()
