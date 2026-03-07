## 部署流程
1. 更新项目
```shell
git pull
```
2. 查看git log获取最新commit_id
```shell
git log --oneline
```

3. 部署项目
```shell
make commit_id=<commit_id>
```
