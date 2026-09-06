repo: {
    work: {branch: "rat/{{agent}}/{{task}}", worktree: "{{repo}}/{{agent}}"}
    delivery: {
        target: "main"
        mode: "merge"
        remote: "origin"
        remoteBranch: "{{branch}}"
        deleteSource: true
    }
    landing: {
        protectedPaths: "(^|/)(\\.github|\\.rk)/"
        maxDiffFiles: 1
        maxDiffLines: 20
        gateTimeout: "30s"
        reviewTimeout: "2m"
        reviewMaxWait: "5m"
    }
}
