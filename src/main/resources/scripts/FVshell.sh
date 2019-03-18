#!/bin/bash
IFS="|"
while read f1 f2
do
        echo "Key   is   : $f1"
        echo "Value   is   : $f2"
        eCollection+=("$f2")
done < /Users/amaraj0/script/Arguments_FileValidation.txt
printf '%s\n' "${eCollection[0]}"
var1=''
for i in "${eCollection[@]}"
do
  var1="$var1$i "
  echo $i
done
echo $var1
#echo $args
var2='spark-submit --class FileValidationTest.Validation.FValidation --master local'
var3="$var2 $var1"
echo $var3
eval "$var3"