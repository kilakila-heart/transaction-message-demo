package com.company.project.biz.service;

import com.alibaba.fastjson.JSON;
import com.company.project.biz.entity.TransferRecord;
import com.company.project.biz.entity.User;
import com.company.project.biz.mapper.UserMapper;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 消费者端业务服务
 * 处理事务消息消费的业务逻辑
 */
@Service
public class ConsumerService {
    
    @Resource
    private UserMapper userMapper;
    
    // 使用内存缓存来记录已处理的转账记录，实现幂等性
    // 注意：在生产环境中，应该使用Redis等分布式缓存，或者创建专门的消费者处理记录表
    private static final ConcurrentHashMap<String, Boolean> processedRecordNos = new ConcurrentHashMap<>();
    
    /**
     * 处理转账消息，为收款用户增加金额
     * 
     * @param messageBody 消息内容
     * @return 处理结果
     */
    @Transactional(rollbackFor = Exception.class)
    public boolean processTransferMessage(String messageBody) {
        try {
            // 解析转账记录
            TransferRecord transferRecord = JSON.parseObject(messageBody, TransferRecord.class);
            
            if (transferRecord == null) {
                System.err.println("=== 消息解析失败 ===");
                System.err.println("消息内容: " + messageBody);
                System.err.println("==================");
                return false;
            }
            
            Long toUserId = transferRecord.getToUserId();
            Long changeMoney = transferRecord.getChangeMoney();
            Long fromUserId = transferRecord.getFromUserId();
            String recordNo = transferRecord.getRecordNo();
            String transactionId = transferRecord.getTransactionId();
            
            System.out.println("=== 开始处理转账业务 ===");
            System.out.println("转账人ID: " + fromUserId);
            System.out.println("收款人ID: " + toUserId);
            System.out.println("转账金额: " + changeMoney);
            System.out.println("转账流水号: " + recordNo);
            System.out.println("事务ID: " + transactionId);
            
            // 参数校验
            if (toUserId == null || changeMoney == null || changeMoney <= 0) {
                System.err.println("=== 转账参数错误 ===");
                System.err.println("收款人ID: " + toUserId);
                System.err.println("转账金额: " + changeMoney);
                System.err.println("==================");
                return false;
            }
            
            // 幂等性检查：检查是否已经处理过这个转账记录
            // 使用recordNo作为键，因为它是唯一的，而transactionId可能为null
            if (recordNo != null && processedRecordNos.containsKey(recordNo)) {
                System.out.println("=== 转账记录已处理过，跳过处理 ===");
                System.out.println("转账流水号: " + recordNo);
                System.out.println("事务ID: " + transactionId);
                System.out.println("==================");
                return true; // 已经处理过，返回成功
            }
            
            // 检查收款用户是否存在
            User toUser = userMapper.selectById(toUserId);
            if (toUser == null) {
                System.err.println("=== 收款用户不存在 ===");
                System.err.println("收款人ID: " + toUserId);
                System.err.println("==================");
                return false;
            }
            
            System.out.println("=== 收款用户信息 ===");
            System.out.println("用户ID: " + toUser.getId());
            System.out.println("当前余额: " + toUser.getMoney());
            System.out.println("==================");
            
            // 执行收款用户增加金额操作
            int result = userMapper.addMoney(toUserId, changeMoney);
            if (result > 0) {
                // 查询更新后的用户信息
                User updatedUser = userMapper.selectById(toUserId);
                System.out.println("=== 转账成功 ===");
                System.out.println("收款人ID: " + toUserId + " 增加金额: " + changeMoney);
                System.out.println("转账前余额: " + toUser.getMoney());
                System.out.println("转账后余额: " + (updatedUser != null ? updatedUser.getMoney() : "未知"));
                System.out.println("转账流水号: " + recordNo);
                System.out.println("==================");
                
                // 记录已处理的转账记录（在事务提交后记录）
                if (recordNo != null) {
                    processedRecordNos.put(recordNo, true);
                }
                
                return true;
            } else {
                System.err.println("=== 转账失败 ===");
                System.err.println("收款人ID: " + toUserId + " 增加金额失败");
                System.err.println("转账流水号: " + recordNo);
                System.err.println("==================");
                return false;
            }
            
        } catch (Exception e) {
            System.err.println("处理转账消息时发生异常: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    /**
     * 清理已处理的转账记录缓存（可选，用于内存管理）
     */
    public void clearProcessedRecordNos() {
        processedRecordNos.clear();
        System.out.println("=== 已清理处理过的转账记录缓存 ===");
    }
    
    /**
     * 获取已处理的转账记录数量（用于监控）
     */
    public int getProcessedRecordCount() {
        return processedRecordNos.size();
    }
}
