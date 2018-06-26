title Merge branches with origin/master
echo Merge branches with origin/master

FOR %%A  IN (li_adelaide_2016,li_bris_2016,li_canberra_2016,li_darwin_2016,li_hobart_2016,li_melb_2016,li_melb_2016_psma,li_perth_2016,li_syd_2016) DO (
  git fetch && git checkout %%A
  git pull
  git merge origin/master
  git commit -a -m "merged branch %%A with master"
  git push
)

@pause
