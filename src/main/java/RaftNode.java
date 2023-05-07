
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.concurrent.*;

import beans.RaftLog;
import beans.RaftRole;
import beans.RestResult;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RaftNode extends UnicastRemoteObject implements RaftRMI {
    //
    private final ExecutorService es = Executors.newFixedThreadPool(8);

    private volatile RaftRole role = RaftRole.FOLLOWER;
    private volatile int msPassed = 0; // 计时
    private int followerExpire = 150; // follower -> candidate 等待时间
    private int candidatExpire = 150; // candidate -> new candidate
    private final int leaderExpire = 10; // 心跳信号的间隔

    // Raft 持久状态
    private int currentTerm = 1;
    private int voteFor = 0;
    private List<String> logs;
    // Raft 暂态
    private int leaderID = 0; // 当前 leader 的ID
    private int serverID = 0; // 当前服务器的 ID
    private int commitIndex;
    private int lastApplied; // 上位机使用
    // Raft leader state
    private final int[] nextIndex = new int[5];
    private final int[] matchIndex = new int[5];

    public RaftNode(int id) throws RemoteException {
        serverID = id;
        roleSwitch(RaftRole.CANDIDATE);
        roleSwitch(RaftRole.FOLLOWER);
        while (true) {
            try {
                Thread.sleep(1000); // 休眠 1ms 调试时设置为1s
                msPassed ++;
            } catch (InterruptedException e) {
                log.error(e.toString());
            }
            switch (role) {
                case FOLLOWER -> { // follower 超时，开启选举操作
                    if (msPassed >= followerExpire) {
                        msPassed = 0;
                        roleSwitch(RaftRole.CANDIDATE);
                        voteFor = serverID; // 第一票投给自己
                        while (role == RaftRole.CANDIDATE) { // 每次轮选举持续 candidateExpire 时间
                            CountDownLatch cdl = new CountDownLatch(2);
                            currentTerm ++;
                            for (int i = 0; i < 5; i ++) {
                                if (i == serverID) continue;
                                requestVoteLauncher(i, cdl);
                            }
                            try {
                                boolean voteResult = cdl.await(candidatExpire, TimeUnit.MILLISECONDS);
                                if (voteResult) { // 投票成功 转化为leader并进行leader的初始化工作，开始同步数据
                                    roleSwitch(RaftRole.LEADER);
                                    Arrays.fill(matchIndex, -1);
                                    Arrays.fill(nextIndex, logs.size());
                                }
                            } catch (InterruptedException e) {
                                log.error(e.toString());
                            }
                        }
                    }
                }
                case LEADER -> { // leader 发布同步命令，若无需同步则视为心跳信号
                    if (msPassed >= leaderExpire) {
                        msPassed = 0;
                        for (int i = 0; i < 5; i ++) {
                            if (i == serverID) continue;
                            appendEntriesLauncher(i); // 数据同步。
                        }
                    }
                }
            }
        }

    }

    /**
     * if (term == currentTerm && logs[prevLogIndex] exits and matches prevLogTerm), return true, else return false.
     * appendEntries is a process that 1. clear the out-dated entries and 2. append new entries.
     * @param term term of the current leader.
     * @param leaderID leader id.
     * @param prevLogIndex the log index that is confirmed by the leader.
     * @param prevLogTerm the log term of the corresponding log index.
     * @param nxtLogs logs to be transmitted.
     * @param leaderCommit the committed index.
     * @return RestResult
     */
    public synchronized RestResult appendEntries(int term, int leaderID, int prevLogIndex, int prevLogTerm, List<String> nxtLogs, int leaderCommit) {
        msPassed = 0; // 心跳信号送达，计数器置零
        commitIndex = leaderCommit;
        this.leaderID = leaderID;
        roleSwitch(RaftRole.FOLLOWER);
        if (term < currentTerm) {
            return new RestResult(currentTerm, false);
        }
        if (prevLogIndex == -1) { // 当前logs为空
            return new RestResult(currentTerm, true);
        } else if (prevLogIndex > logs.size()) { //
            return new RestResult(currentTerm, false);
        } else if (prevLogIndex == logs.size()) {
            logs.addAll(nxtLogs);
            return new RestResult(currentTerm, false);
        } else {
            RaftLog raftLog = (RaftLog) JSON.parse(logs.get(prevLogIndex));
            if (raftLog.getTerm() == prevLogTerm) { // 当前最新的 log 与 leader 匹配，则返回 true
                return new RestResult(currentTerm, true);
            } else { // prevLogIndex 不匹配，删除不匹配之后的所有日志项
                int ts = logs.size();
                for (int di = prevLogIndex; di < ts; di ++) {
                    logs.remove(logs.size() - 1);
                }
                return new RestResult(currentTerm, false);
            }
        }
    }

    private void appendEntriesLauncher(int i) {
        if (role != RaftRole.LEADER) return;
        LinkedList<String> nxtLogs = new LinkedList<>(); // 此处可考虑使用下标，是一个空间和时间的 trade-off
        try {
            Registry registry = LocateRegistry.getRegistry("localhost", Registry.REGISTRY_PORT);
            RaftRMI raftRMI = (RaftRMI) registry.lookup("/raft/" + i);
            nextIndex[i] = logs.size() - 1;
            RaftLog raftLog = (RaftLog) JSON.parse(logs.get(nextIndex[i]));
            RestResult r = raftRMI.appendEntries(currentTerm, serverID, nextIndex[i], raftLog.getTerm(), null, commitIndex);
            while (!r.isResult() && nextIndex[i] > -1) { // 清理 server i 的旧日志直到日志为空
                if (nextIndex[i] < logs.size()) {
                    nxtLogs.addFirst(logs.get(nextIndex[i]));
                }
                nextIndex[i] --;
                raftLog = (RaftLog) JSON.parse(logs.get(nextIndex[i]));
                r = raftRMI.appendEntries(currentTerm, serverID, nextIndex[i], raftLog.getTerm(), null, commitIndex);
                if (r.getTerm() > currentTerm) { // 出现新的任期，修改当前服务器角色为FOLLOWER并退出同步线程
                    roleSwitch(RaftRole.FOLLOWER);
                    return;
                }
            }
            // 当前同步的日志在下一个周期确认
            // prevLogIndex确认，更新 commitIndex，清空传输队列
            matchIndex[i] = nextIndex[i];
            nextIndex[i] ++;
            int[] tmp = Arrays.copyOf(matchIndex, 5);
            Arrays.sort(tmp);
            commitIndex = Math.max(commitIndex, tmp[3]); // commitIndex 单增
            nextIndex[i] ++;
            // 限时添加新日志
            Future<RestResult> fr = es.submit(()->{
                return raftRMI.appendEntries(currentTerm, serverID, nextIndex[i], 0, nxtLogs, commitIndex); // prevLogTerm不需要
            });
            // 定时执行同步任务，若为在限定时间内同步成功则不进行matchIndex的更新
            try {
                fr.get(leaderExpire>>1, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) { // 同步超时，限制 nxtLog 的最大长度
                log.error(e.toString());
            } catch (Exception ee) {
                log.error(ee.toString());
            }

        } catch (RemoteException | NotBoundException re) {
            log.error(re.toString());
        }

    }

    /**
     *
     * @param term candidate term.
     * @param candidatID candidate id.
     * @param lastLogIndex the latest log index of the current candidate.
     * @param lastLogTerm the lastest log term of the current candidate.
     * @return RestResult
     */
    public RestResult requestVote(int term, int candidatID, int lastLogIndex, int lastLogTerm) {
        if (currentTerm < term) {
            currentTerm = term;
            roleSwitch(RaftRole.FOLLOWER);
            voteFor = 0;
        }
        if (term < currentTerm || (voteFor != 0 && voteFor != candidatID)) { // 候选人的term已过期或当前服务器已投其他候选人
            return new RestResult(currentTerm, false);
        } else if (lastLogIndex >= logs.size()) { // 候选人的日志多于当前服务器日志
            voteFor = candidatID;
            return new RestResult(currentTerm, true);
        } else { // 候选人的最新日志 term 高于当前
            RaftLog prevLog = (RaftLog) JSON.parse(logs.get(lastLogIndex));
            if (prevLog.getTerm() <= lastLogTerm) {
                voteFor = candidatID;
                return new RestResult(currentTerm, true);
            }
        }
        return new RestResult(currentTerm, false);
    }

    private void requestVoteLauncher(int i, CountDownLatch cdl) {
        if (role != RaftRole.CANDIDATE) return;
        try {
            Registry registry = LocateRegistry.getRegistry("localhost", Registry.REGISTRY_PORT);
            RaftRMI raftRMI = (RaftRMI) registry.lookup("/raft/" + i);
            Future<RestResult> fr = es.submit(()-> {
                    if (logs.size() == 0) {
                        return raftRMI.requestVote(currentTerm, serverID, 0, 0);
                    } else {
                        RaftLog lastLog = (RaftLog) JSON.parse(logs.get(logs.size() - 1));
                        return raftRMI.requestVote(currentTerm, serverID, logs.size() - 1, lastLog.getTerm());
                    }
            });
            if (fr.get(candidatExpire>>1, TimeUnit.MILLISECONDS).isResult()) {
                cdl.countDown();
            }
        } catch (Exception e) {
            log.error(e.toString());
        }
    }

    /**
     * perform exchange of role. assign new random expire time.
     * @param newRole
     */
    private void roleSwitch(RaftRole newRole) {
        Random rand = new Random();
        switch (newRole) {
            case LEADER -> {
                role = RaftRole.LEADER;
            }
            case FOLLOWER -> {
                role = RaftRole.FOLLOWER;
                followerExpire = rand.nextInt(50) + 50;
            }
            case CANDIDATE -> {
                role = RaftRole.CANDIDATE;
                candidatExpire = rand.nextInt(50) + 50;
            }
        }
    }
}
