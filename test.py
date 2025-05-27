import happybase

class HBaseClient:
    def __init__(self, host='localhost', port=9090):
        try:
            self.connection = happybase.Connection(host=host, port=port)
            print(f"Connected to HBase at {host}:{port}")
        except Exception as e:
            print(f"Failed to connect to HBase: {e}")
            print("Make sure HBase Thrift server is running")
            raise

    def list_tables(self):
        """List all tables in HBase"""
        tables = self.connection.tables()
        print("Available tables:")
        for table in tables:
            print(f"  - {table.decode('utf-8')}")
        return tables
    
if __name__ == "__main__":
    client = HBaseClient()
    
    client.list_tables()