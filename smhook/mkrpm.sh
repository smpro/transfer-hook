#!/bin/bash 

# Usage: ./mkrpm.sh

VERSION=1.0
RELEASE=17

echo 'You are trying to build an RPM for smhook version:',$VERSION,' and release: '$RELEASE   

rpmdev-setuptree

cd ~/rpmbuild/SOURCES/
git clone git@github.com:smpro/transfer-hook.git
cd transfer-hook
git checkout devel

#Get list of files in smhook
for i in `find smhook -type f`; do 
    if [ $i != 'smhook/mkrpm.sh' ] && [ $i != 'smhook/TEMP.spec' ] && [ $i != 'smhook/config/smeord_priority.conf' ] && [ $i != 'smhook/config/smhookd_priority.conf' ]; then 
	echo $i >> files_TEMP.list; 
    fi
done

sed -e "s\smhook/\/opt/python/smhook/\g" files_TEMP.list > files.list
sed -e "s/__VERSION__/$VERSION/g" ~/rpmbuild/SOURCES/transfer-hook/smhook/TEMP.spec > TEMP_$VERSION.spec
sed -e "s/__RELEASE__/$RELEASE/g" TEMP_$VERSION.spec > TEMP_$VERSION$RELEASE.spec
sed -e "s\MYDIR\~/rpmbuild/SOURCES/\g" TEMP_$VERSION$RELEASE.spec > smhook.spec

tar --exclude='TEMP.spec' --exclude='mkrpm.sh' --exclude='smhookd_priority.conf' --exclude='smeord_priority.conf' -cvzf smhook.tgz smhook
mv ~/rpmbuild/SOURCES/transfer-hook/smhook.tgz ../
mv ~/rpmbuild/SOURCES/transfer-hook/files.list  ~/rpmbuild/SPECS  
mv ~/rpmbuild/SOURCES/transfer-hook/smhook.spec ~/rpmbuild/SPECS
rm -rf ~/rpmbuild/SOURCES/transfer-hook/
cd ~/rpmbuild/SPECS/
rpmbuild -ba smhook.spec

# Now put the built RPM elsewhere and remove the dir
mkdir ~/SMHOOK_RPM
cp ~/rpmbuild/RPMS/x86_64/* ~/SMHOOK_RPM/
cp ~/rpmbuild/SPECS/smhook.spec ~/SMHOOK_RPM/
rm -rf ~/rpmbuild/