x=0;
while [ $x  -lt 256 ];
do
if [ "$x" -lt 96 ];then
./btask -h127.0.0.1 -p2066 -m0 -q1 2 >&1 > 01.txt &
fi
#./btask -h127.0.0.1 -p2066 -m0 -q1 2 >&1 > 01.txt &
./btask -h127.0.0.1 -p2066 -m1 -q2 2 >&1 > 12.txt &
./btask -h127.0.0.1 -p2066 -m2 -q3 2 >&1 > 23.txt &
./btask -h127.0.0.1 -p2066 -m3 -q0 2 >&1 > 30.txt &
((x++));
done;
