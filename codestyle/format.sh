#!/bin/sh

baseDir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"/../
mergeBase=`git merge-base origin/polardbx_develop HEAD`
files=`git diff --name-only ${mergeBase} | grep '.java' | grep -v 'polardbx-calcite' | grep -v 'polardbx-orc'  | grep -v 'polardbx-rpc/src/main/java/com/mysql/cj'| grep -v 'com/alibaba/polardbx/rpc/cdc'| grep -v 'com/alibaba/polardbx/rpc/columnar'| xargs -I {} echo ${baseDir}{}`

# 按优先级查找 IDEA 可执行文件
if [ -f "/Applications/IntelliJ IDEA.app/Contents/MacOS/idea" ]; then
    IDEA_BIN="/Applications/IntelliJ IDEA.app/Contents/MacOS/idea"
elif [ -f "/Users/$USER/Applications/IntelliJ IDEA.app/Contents/MacOS/idea" ]; then
    IDEA_BIN="/Users/$USER/Applications/IntelliJ IDEA.app/Contents/MacOS/idea"
elif [ -f "$HOME/Library/Application Support/JetBrains/Toolbox/scripts/idea" ]; then
    IDEA_BIN="$HOME/Library/Application Support/JetBrains/Toolbox/scripts/idea"
else
    echo "ERROR: IntelliJ IDEA not found. Please set IDEA_BIN manually."
    exit 1
fi

count=0
batchFile=''
for file in $files; do
    if [ -f "$file" ]; then
        count=$((($count + 1) % 100))
        batchFile=$batchFile' '$file
        if [[ $count -eq 0 ]]; then
            "$IDEA_BIN" format -s ${baseDir}'codestyle/codestyle-idea.xml' -m '*.java' ${batchFile}
            batchFile=''
        fi
        trap "echo Exited!; exit;" SIGINT SIGTERM
    fi
done

if [[ ! -z $batchFile ]]; then
	"$IDEA_BIN" format -s ${baseDir}'codestyle/codestyle-idea.xml' -m '*.java' ${batchFile}
fi


