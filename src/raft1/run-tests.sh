#!/bin/bash
count=0
rm $2-*.txt

for ((i=0; i<$1; i+=$3))
do
    echo "$count/$1"

    for ((j=1; j<=$3; j++))
    do {
        filename="$2-res-$j.txt"
        LOG_LEVEL=$4 go test -run $2 > $filename
        current=$(grep -o 'ok' $filename | wc -l)
        num=$[i+j]
        if [ $current -gt 0 ]; then
            echo "($num) test $2 passed once"
        else
            echo "($num) !!!error happened when running test $2!!!"
            newFilename="$2-error-$num.txt"
            mv $filename $newFilename
        fi
    } &
    done
    wait

    count=$((${count} + $3))

done

echo "$2 tests finished: $count/$1"
failed=$(ls $2-error*.txt |wc -l)
echo "test failed: $failed/$1"

rm $2-res-*.txt
