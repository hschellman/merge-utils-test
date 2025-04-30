export TEMP=$PWD
export HERE=$PWD/..
cd $HERE/src/merge_utils
echo $PWD
pydeps metacat_utils.py -T jpg --include-missing --cluster --max-bacon 3 --rankdir BT
pydeps io_utils.py -T jpg --include-missing --cluster --max-bacon 3 --rankdir BT
pydeps file_utils.py -T jpg --include-missing --cluster --max-bacon 3 --rankdir BT
pydeps rucio_utils.py -T jpg --include-missing --cluster --max-bacon 3 --rankdir BT
#mv file_utils.jpg $HERE/docs
#mv rucio_utils.jpg $HERE/docs
mv *.jpg $HERE/docs
chmod +x *.jpg
cd $TEMP
echo $PWD
