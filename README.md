# 大数据实验- Hadoop网络日志分析

## 一、仓库使用

### 1. 克隆仓库

```shell
git clone git@github.com:raindrops-0199/website_log_analysis.git
```

### 2. 建立自己的分支

克隆仓库后是默认main分支，具体的开发在各自的分支中进行，需要新建自己的分支

```shell
git checkout -b xxx
# xxx是自己的分支
```

***分支名分配(也可以自己添加分支，如`cyp-test`之类的)：***

刘凯瑞：`lkr`

夏岩: `xy`

程友朋: `cyp`

### 3. 代码提交

在自己的分支上完成部分代码后，可提交至GitHub

```shell
git push origin xxx
# xxx是自己的分支
```

注意不要push到main分支上

各个部分完成后的数据可上传至仓库，方便其他人使用。

### 4. 获取其他分支代码

有时需要使用其他分支的代码或数据

```shell
git pull origin xxx
# xxx是要获取代码的分支名
```

### 4. 分支合并

目前各个分支可分开进行，无需合并。在需要合并或学期结束时会合并至`main`分支

## 二、仓库结构

```shell
.
├── .gitignore
├── README.md
├── task1
├── task2
├── task3
├── task4
├── task5
└── task6
```

各自的部分在相应文件夹内完成。

刚clone下来时是没有task文件夹的，需要自己新建自己负责的任务的task文件夹。

例如`lkr`分支会是这样的：

```
.
├── .gitignore
├── README.md
├── task3
└── task4
```

其他的task文件夹会在pull别人提交后的分支后出现

***也可以根据实际情况创建文件夹，例如两个任务合并更方便，可以建立`task1-2`之类的***

## 三、备注

### 关于gitignore

- jar包是被ignore了的。在最后提交作业时再统一整理放到一起
- 如果哪个需要上传的文件被ignore了，在`.gitignore`里添加`!xxx`规则，`xxx`就是要包含的文件或规则

