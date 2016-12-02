#!/bin/bash 

# Usage: ./mkrpm.sh

VERSION=1.0
RELEASE=3

echo 'You are trying to build an RPM for smhook version:',$VERSION,' and release: '$RELEASE   

rpmdev-setuptree

cd ~/rpmbuild/SOURCES/
git clone git@github.com:smpro/transfer-hook.git
cd transfer-hook
git checkout pdt

#Get list of files in smhook
for i in `find smhook -type f`; do 
    if [ $i != 'smhook/mkrpm.sh' ] && [ $i != 'smhook/TEMP.spec' ] && [ $i != 'smhook/config/smhookd_priority.conf' ]; then 
	echo $i >> files_TEMP.list; 
    fi
done

sed -e "s\smhook/\/opt/python/smhook/\g" files_TEMP.list > files.list
sed -e "s/__VERSION__/$VERSION/g" ~/rpmbuild/SOURCES/transfer-hook/smhook/TEMP.spec > TEMP_$VERSION.spec
sed -e "s/__RELEASE__/$RELEASE/g" TEMP_$VERSION.spec > TEMP_$VERSION$RELEASE.spec
sed -e "s\MYDIR\~/rpmbuild/SOURCES/\g" TEMP_$VERSION$RELEASE.spec > smhookPDT.spec

tar --exclude='TEMP.spec' --exclude='mkrpm.sh' --exclude 'smhook_priority.conf' -cvzf smhookPDT.tgz smhook
#echo ' test 1'
mv ~/rpmbuild/SOURCES/transfer-hook/smhookPDT.tgz ../
#echo ' test 2'
#pwd
#ls /nfshome0/dhsu/rpmbuild/SOURCES
mv ~/rpmbuild/SOURCES/transfer-hook/files.list  ~/rpmbuild/SPECS  
#echo ' test 3'
#pwd
#ls /nfshome0/dhsu/rpmbuild/SOURCES
cp ~/rpmbuild/SOURCES/transfer-hook/smhookPDT.spec ~
mv ~/rpmbuild/SOURCES/transfer-hook/smhookPDT.spec ~/rpmbuild/SPECS
#echo ' test 4'
#pwd
#ls /nfshome0/dhsu/rpmbuild/SOURCES
rm -rf ~/rpmbuild/SOURCES/transfer-hook/
#echo ' test 5'
#pwd
#ls /nfshome0/dhsu/rpmbuild/SOURCES
cd ~/rpmbuild/SPECS/
#echo ' test 6'
#pwd
#ls /nfshome0/dhsu/rpmbuild/SOURCES
rpmbuild -ba smhookPDT.spec

# Now put the built RPM elsewhere and remove the dir
#echo ' test 7'
rm -rf ~/SMHOOKPDT_RPM
mkdir ~/SMHOOKPDT_RPM
#echo ' test 8'
cp ~/rpmbuild/RPMS/x86_64/* ~/SMHOOKPDT_RPM/
#echo ' test 9'
cp ~/rpmbuild/SPECS/smhookPDT.spec ~/SMHOOKPDT_RPM/
rm -rf ~/rpmbuild/
