CREATE TABLE IF NOT EXISTS user_feature (
    id             STRING COMMENT '用户ID',
    user_name      STRING COMMENT '用户名称',
    age            STRING COMMENT '年龄',
    gender         STRING COMMENT '性别',
    address        STRING COMMENT '住址'
)
COMMENT '用户信息表';