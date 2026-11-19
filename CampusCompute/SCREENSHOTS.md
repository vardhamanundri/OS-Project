# Screenshot commands (run in WSL, inside the CampusCompute folder)

Each partner takes their OWN screenshots in their OWN terminal with their OWN SRN, and builds their own PDF.
Set the prompt first (use your own SRN):

    export SRN='PES1UG25AM440'; export PS1='[$SRN] \w\$ '; clear      # partner: SRN='PES1UG25AM488'

| # | Command(s) |
|---|---|
| S01 | `pwd; uname -a; lsb_release -d` |
| S02 | `gcc --version \| head -1; make --version \| head -1` |
| S03 | `ls -l; ls -l samples` |
| S04 | `make clean; make` |
| S05 | `./campuscompute help` |
| S06 | `./campuscompute sched fcfs` |
| S07 | `./campuscompute sched rr -q 2` |
| S08 | `./campuscompute sched priority` |
| S09 | `./campuscompute sched compare` |
| S10 | `./campuscompute logger 2 2 5` (make sure a "queue FULL" line and logged entries are visible) |
| S11 | `./campuscompute logger 2 2 5 \| tail -8` (shows threads joined, produced = consumed) |
| S12 | `./campuscompute ipc \| head -20` |
| S13 | `./campuscompute ipc \| tail -12` |
| S14 | `./campuscompute banker safe \| tail -14` |
| S15 | `./campuscompute banker unsafe \| tail -10` |
| S16 | `./campuscompute worker normal` |
| S17 | `./campuscompute worker all` |
| S18 | `./campuscompute rlimit all` |
| S19 | `./campuscompute foo; ./campuscompute sched rr -q abc; ./campuscompute sched fcfs -f samples/bad_jobs.txt` |
| S20 | `./campuscompute cleanup; ls /dev/shm \| grep -i campus; pgrep -l campuscompute; echo "no leftovers"` |
| S21 | `./campuscompute demo \| tail -16` |
| S22 | `ls -l campuscompute.log; head -10 campuscompute.log` |
| S23 | `ls -l campuscompute.c Makefile README.md campuscompute` |
| S24 | `cd ~ && zip -r $SRN.zip CampusCompute -x '*.log' && unzip -l $SRN.zip && unzip -t $SRN.zip` |

Notes
* Run S22 after S21 or S10 so `campuscompute.log` exists (`demo` also creates it).
* If the output is longer than the window, scroll so the command and the key lines are both visible.
* S24: adjust the folder path if the project is not in `~/CampusCompute`. The ZIP will contain your executable
  unless you add `-x 'CampusCompute/campuscompute'`.

# Build the PDF
Save your real screenshots as S01.png ... S24.png in one folder with `captions.txt` and `make_pdf.py`, then:

    sudo apt install -y python3-pil        # or: pip install pillow --break-system-packages
    python3 make_pdf.py $SRN      # -> <your SRN>.pdf
