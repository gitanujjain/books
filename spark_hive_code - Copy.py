def write_to_hive(df, epoch_id):
    try:
        print("DF table operations")
        operation = df.select("payload.op").collect()
        op_out = ""
        id_a = ""
        if len(operation) > 0:
            op_out = operation[0][0]
            print(op_out)
            if op_out == "C":
                ids = df.select("payload.after.id").collect()
                id_a = ids[0][0]
                
                first_name = df.select("payload.after.first_name").collect()
                last_name = df.select("payload.after.last_name").collect()
                email_id = df.select("payload.after.email").collect()
                first_name = first_name[0][0]
                last_name = last_name[0][0]
                email_id = email_id[0][0]
                query = f"INSERT INTO  cdc.translab1 values({id_a}, '{first_name}','{last_name}', '{last_name}','{email_id}';"
                print(query)
                spark.sql(query)
                print ("record is created")
            
            if op_out == "r":
                ids = df.select("payload.after.id").collect()
                id_a = ids[0][0]
                print(id_a)
                print("operation is r")
                df4 = df.select("payload.after.id", "payload.after.first_name", "payload.after.last_name", "payload.after.email")
                # Write to MySQL
                df4.write.mode("append").format("hive").saveAsTable("cdc.translab1")
            if op_out == "d":
                print("delete record")
                print ("operation is d")
                ids = df.select("payload.before.id").collect()
                print("delete record: ")
                id=""
                if (len(ids) > 0):
                    id = ids[0][0]
                    query = f"DELETE FROM cdc.translab1 WHERE id='{id}'"
                    print(query)
                    print(id)
                    spark.sql(query)
                print ("record is deleted")
            if op_out == "null":
                print("id is null")
            elif op_out == "u":    
                print ("operation is u")
                ids = df.select("payload.before.id").collect()
                after_ids = df.select("payload.after.id").collect()
                first_name = df.select("payload.after.first_name").collect()
                last_name = df.select("payload.after.last_name").collect()
                email_id = df.select("payload.after.email").collect()
                if (len(ids) > 0):
                    before_id = ids[0][0]
                    id_a = after_ids[0][0]
                    first_name = first_name[0][0]
                    last_name = last_name[0][0]
                    email_id = email_id[0][0]
                    query = f"UPDATE cdc.translab1 SET id = {id_a}, first_name = '{first_name}', last_name = '{last_name}', last_name = '{last_name}', email = '{email_id}'where id = {before_id};"
                    print(query)
                    spark.sql(query)
                    print ("record is updated")
    except Exception as e:

        print(e)
		
		

