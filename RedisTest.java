import org.apache.commons.lang.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Tuple;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * redis 数据导出功能
 * @author : jingp
 * @Description: TODO
 * @date Date : 2022年04月01日 20:00
 */
public class RedisTest {

    //redis超时时间
    private static final int timout = 1000;
    /**
     * redis源数据配置
     */
    //源redis-ip地址
    private static final String source_host = "127.0.0.1";
    //源redis端口
    private static final int source_port = 6379;
    //源redis密码
    private static final String source_password = null;
    //源数据库
    private static final int source_database = 0;

    /**
     * redis目标数据配置
     */
    //目标redis-ip地址
    private static final String target_host = "127.0.0.1";
    //目标redis端口
    private static final int target_port = 6379;
    //目标redis密码
    private static final String target_password = null;
    //目标数据库
    private static final int target_database = 2;


    private static Jedis source = null;
    private static Jedis target = null;
    static {
        source = new JedisPool(new JedisPoolConfig(), source_host, source_port,timout,source_password,source_database).getResource();
        target = new JedisPool(new JedisPoolConfig(), target_host, target_port,timout,target_password,target_database).getResource();
    }

    public static void main(String[] args) {
        try {
            Set<String> keys = source.keys("*");
            //数据同步
            redisTool(keys);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally{
            //关闭连接
            source.quit();
            target.quit();
        }
    }

    /**
     * redis数据转换处理
     * @author : jingp
     * @param keys
     * @date Date : 2022年04月01日 20:15
     */
    public static void redisTool(Set<String> keys){
        for (String key : keys) {
            if(source == null || !source.exists(key) || StringUtils.isEmpty(key)){
                System.out.println("key:"+ key +" 不存在");
                continue;
            }
            //获取key对应的数据类型
            String type = source.type(key);
            //判断key值属于哪一种类型，分别进行处理
            if(type.equals(RedisTypeEnum.STRING.getType())){
                target.set(key,source.get(key));
                System.out.println("key:"+ key +"，type: "+ type +" ，同步成功");
                continue;
            }
            if(type.equals(RedisTypeEnum.LIST.getType())){
                List<String> lrange = source.lrange(key, 0, -1);
                target.rpush(key,lrange.toArray(new String[lrange.size()]));
                System.out.println("key:"+ key +"，type: "+ type +" ，同步成功");
                continue;
            }
            if(type.equals(RedisTypeEnum.SET.getType())){
                Set<String> smembers = source.smembers(key);
                target.sadd(key,smembers.toArray(new String[smembers.size()]));
                System.out.println("key:"+ key +"，type: "+ type +" ，同步成功");
                continue;
            }
            if(type.equals(RedisTypeEnum.ZSET.getType())){
                Set<Tuple> tuples = source.zrangeWithScores(key, 0, -1);
                target.zadd(key,tuples.stream().collect(Collectors.toMap(Tuple::getElement,Tuple::getScore)));
                System.out.println("key:"+ key +"，type: "+ type +" ，同步成功");
                continue;
            }
            if(type.equals(RedisTypeEnum.HASH.getType())){
                Map<String, String> hashMap = source.hgetAll(key);
                target.hmset(key,hashMap);
                System.out.println("key:"+ key +"，type: "+ type +" ，同步成功");
                continue;
            }
            System.out.format("\33[41;4m" + "未处理key:"+ key +"，type: "+ type);
        }
    }

    /**
     * redis数据类型
     * @author : jingp
     * @date Date : 2022年04月01日 20:23
     */
    enum RedisTypeEnum{
        ZSET("zset"),
        REJSON("rejson"),
        HASH("hash"),
        SET("set"),
        LIST("list"),
        STREAM("stream"),
        STRING("string");

        String type;

        RedisTypeEnum(String type){
            this.type = type;
        }

        public String getType() {
            return type;
        }
    }
}

