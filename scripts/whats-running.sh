#!/usr/bin/env bash
# What is ACTUALLY running here, as opposed to what is checked out.
#
# Written 2026-08-19 after two agents spent a morning disagreeing about which
# code was live on which host. Both were partly right, which is the point: for
# every component there are THREE versions, and they diverge silently.
#
#   remote master   what has been pushed
#   checkout HEAD   what is on this disk
#   the process     what the running JVM loaded at start
#
# git can only see the first two, and `git log --all` means all LOCAL refs, so
# on an unfetched tree it returns a confident empty answer. Neither tells you
# what a long-lived server process is executing -- a JVM started before a pull
# keeps running the old code with a new checkout sitting beside it.
#
# So the last section probes the LIVE SERVICE for behaviour that only exists
# after particular commits. That is the only section that answers the question
# people actually ask. Test the behaviour; do not infer it from the ref.
#
# Usage:  bash scripts/whats-running.sh "ZONE (production)"
SITE="$1"
echo "### $SITE"
for r in futon1b futon3c; do
  d=/home/joe/code/$r
  [ -d "$d/.git" ] || { echo "  $r: absent"; continue; }
  head=$(git -C "$d" log -1 --format='%h %cs' 2>/dev/null)
  rem=$(git -C "$d" ls-remote origin master 2>/dev/null | cut -c1-8)
  fetch=$(stat -c %y "$d/.git/FETCH_HEAD" 2>/dev/null | cut -c1-16)
  dirty=$(git -C "$d" status --porcelain -uall 2>/dev/null | wc -l)
  unpushed=$(git -C "$d" log --oneline origin/master..HEAD 2>/dev/null | wc -l)
  echo "  $r"
  echo "    checkout HEAD  : $head   (dirty $dirty, $unpushed ahead of its LOCAL origin ref)"
  echo "    remote master  : ${rem:-unreachable}"
  echo "    last fetch     : ${fetch:-never}"
done
pid=$(systemctl --user show -p MainPID --value c7-futon1b.service 2>/dev/null)
[ -z "$pid" -o "$pid" = "0" ] && pid=$(systemctl --user show -p MainPID --value futon1b-server.service 2>/dev/null)
echo "  futon1b PROCESS"
echo "    started        : $(ps -o lstart= -p "$pid" 2>/dev/null | sed 's/^ *//' || echo 'not running')"
echo "  futon1b LIVE BEHAVIOUR (what the process actually has, not what git says)"
u=http://127.0.0.1:7073/api/alpha/evidence/text-search
a=$(curl -s -m 120 "$u?df=memory" 2>/dev/null)
b=$(curl -s -m 120 "$u?df=memory&type=:memory" 2>/dev/null)
if [ "$a" = "$b" ]; then echo "    scoped ?df=    : NO  (filters ignored -> pre-e05183e)"
else echo "    scoped ?df=    : YES (filters honoured -> e05183e or later)"; fi
if curl -s -m 120 "$u?q=evidence&limit=1&hydrate=false" 2>/dev/null | grep -q ':ids'; then
  echo "    :ids in search : YES (-> 0be5a7f or later)"
else echo "    :ids in search : NO  (-> pre-0be5a7f)"; fi
if curl -s -m 120 "$u?stats=true" 2>/dev/null | grep -q ':errors-window'; then
  echo "    labelled stats : YES (-> 5c00129 or later)"
else echo "    labelled stats : NO  (-> pre-5c00129)"; fi
