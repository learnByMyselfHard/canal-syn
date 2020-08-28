package com.lai.canalsyn.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;
import java.util.Map;

/**
 * @ Author : lai
 * @ Date   : created in  2020/4/23 13:57
 * @ Description :
 */
@Mapper
public interface DynasticTableMapper  {
    @Select("SELECT DISTINCT t.table_name, t.TABLE_SCHEMA \n" +
            "FROM information_schema.TABLES t\n" +
            " WHERE t.table_name = #{tableName} AND t.TABLE_SCHEMA = #{databaseName}")
    String isExist(@Param("databaseName") String databaseName, @Param("tableName") String tableName);

    @Select("select * from ${databaseName}.${tableName}")
    List<Map<String,Object>>  getInfos(@Param("databaseName") String databaseName, @Param("tableName") String tableName);
}
