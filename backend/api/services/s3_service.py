"""AWS S3 service for pipeline data management"""
import boto3
from io import BytesIO
import pandas as pd
from django.conf import settings


class S3Service:
    """Manages S3 operations for pipeline data"""
    
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            region_name=settings.AWS_S3_REGION_NAME,
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        )
        self.bucket_name = settings.AWS_S3_BUCKET_NAME
    
    def read_csv_to_df(self, s3_key: str) -> pd.DataFrame:
        """
        Read CSV from S3 and return as pandas DataFrame
        
        Args:
            s3_key: Path to file in S3 (e.g., "pipeline-data/noon-namshi.csv")
        
        Returns:
            pandas.DataFrame
        """
        try:
            obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=s3_key)
            df = pd.read_csv(BytesIO(obj['Body'].read()))
            print(f"✓ Successfully read {s3_key} from S3 ({len(df)} rows)")
            return df
        except Exception as e:
            print(f"✗ Error reading {s3_key}: {str(e)}")
            raise
    
    def delete_file(self, s3_key: str) -> bool:
        """
        Delete a file from S3
        
        Args:
            s3_key: Path to file in S3
        
        Returns:
            bool: True if successful
        """
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=s3_key)
            print(f"✓ Deleted old file: {s3_key}")
            return True
        except Exception as e:
            print(f"⚠ Warning deleting {s3_key}: {str(e)}")
            return False
    
    def file_exists(self, s3_key: str) -> bool:
        """Check if file exists in S3"""
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except:
            return False
    
    def upload_file(self, local_path: str, s3_key: str) -> bool:
        """
        Upload file to S3
        
        Args:
            local_path: Path to local file
            s3_key: Destination path in S3
        
        Returns:
            bool: True if successful
        """
        try:
            self.s3_client.upload_file(local_path, self.bucket_name, s3_key)
            print(f"✓ Uploaded to S3: {s3_key}")
            return True
        except Exception as e:
            print(f"✗ Error uploading: {str(e)}")
            raise


# Global instance
s3_service = S3Service()
