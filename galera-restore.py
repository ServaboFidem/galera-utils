#!/usr/bin/env python3
"""
Galera Node MySQL Monitor and Recovery Script

This script monitors the status of mysqld service on a Galera node,
detects crashes, and attempts recovery if necessary.
"""

import subprocess
import os
import sys
import re
import time
from pathlib import Path


def check_mysqld_status():
    """Check if mysqld service is running."""
    try:
        result = subprocess.run(
            ['systemctl', 'is-active', 'mysqld'],
            capture_output=True,
            text=True
        )
        return result.stdout.strip() == 'active'
    except Exception as e:
        print(f"Error checking mysqld status: {e}")
        return False


def check_crash_signal(log_file='/var/log/mysql/error.log', lines=200):
    """Check if 'mysqld got signal 11' appears in the last N lines of error log."""
    try:
        # Use tail to get last N lines efficiently
        result = subprocess.run(
            ['tail', '-n', str(lines), log_file],
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            print(f"Error reading log file: {result.stderr}")
            return False
            
        return 'mysqld got signal 11' in result.stdout
    except Exception as e:
        print(f"Error checking crash signal: {e}")
        return False


def check_grastate_seqno(grastate_file='/var/lib/mysql/grastate.dat'):
    """Parse grastate.dat and check if seqno is -1."""
    try:
        with open(grastate_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith('seqno:'):
                    # Split by colon and get the value after whitespace
                    parts = line.split(':', 1)
                    if len(parts) == 2:
                        seqno_value = parts[1].strip()
                        return seqno_value == '-1'
        return False
    except Exception as e:
        print(f"Error reading grastate.dat: {e}")
        return False


def check_sudo_permissions():
    """Check if the current user has sudo permissions."""
    try:
        # Try to run a simple sudo command
        result = subprocess.run(
            ['sudo', '-n', 'true'],
            capture_output=True
        )
        return result.returncode == 0
    except Exception:
        return False


def run_wsrep_recover():
    """Run mysqld --wsrep-recover command as mysql user."""
    try:
        print("Running wsrep recovery...")
        result = subprocess.run(
            ['sudo', '-u', 'mysql', 'mysqld', '--wsrep-recover'],
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            print(f"Warning: wsrep-recover returned non-zero exit code: {result.returncode}")
            print(f"stderr: {result.stderr}")
        else:
            print("wsrep-recover completed successfully")
        
        return True
    except Exception as e:
        print(f"Error running wsrep-recover: {e}")
        return False


def parse_wsrep_position(log_file='/var/log/mysql/error.log', lines=50):
    """Parse the last occurrence of wsrep position from error log."""
    try:
        # Get last N lines from log
        result = subprocess.run(
            ['tail', '-n', str(lines), log_file],
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            print(f"Error reading log file: {result.stderr}")
            return None, None
        
        # Find all lines containing "(wsrep position:"
        wsrep_pattern = r'\(wsrep position:\s*([^)]+)\)'
        matches = re.findall(wsrep_pattern, result.stdout)
        
        if matches:
            # Get the last match
            last_position = matches[-1].strip()
            
            # Split by colon to get gid and seqno
            parts = last_position.split(':', 1)
            if len(parts) == 2:
                gid_string = parts[0].strip()
                seqno_string = parts[1].strip()
                return gid_string, seqno_string
        
        return None, None
    except Exception as e:
        print(f"Error parsing wsrep position: {e}")
        return None, None


def update_grastate_seqno(grastate_file='/var/lib/mysql/grastate.dat', new_seqno=''):
    """Update the seqno value in grastate.dat."""
    try:
        # Read all lines
        with open(grastate_file, 'r') as f:
            lines = f.readlines()
        
        # Update seqno line
        updated = False
        for i, line in enumerate(lines):
            if line.strip().startswith('seqno:'):
                lines[i] = f"seqno: {new_seqno}\n"
                updated = True
                break
        
        if not updated:
            print("Error: seqno line not found in grastate.dat")
            return False
        
        # Write back with sudo
        # Create a temporary file first
        temp_file = '/tmp/grastate_temp.dat'
        with open(temp_file, 'w') as f:
            f.writelines(lines)
        
        # Copy with sudo preserving permissions
        result = subprocess.run(
            ['sudo', 'cp', temp_file, grastate_file],
            capture_output=True
        )
        
        # Clean up temp file
        os.remove(temp_file)
        
        if result.returncode == 0:
            print(f"Successfully updated grastate.dat with seqno: {new_seqno}")
            return True
        else:
            print(f"Error updating grastate.dat: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"Error updating grastate.dat: {e}")
        return False


def main():
    """Main monitoring and recovery logic."""
    print("Galera Node MySQL Monitor Started")
    print("=" * 50)
    
    # Check if mysqld is running
    if check_mysqld_status():
        print("✓ mysqld service is running")
        sys.exit(0)
    
    print("✗ mysqld service is not running")
    print("Checking for crash conditions...")
    
    # Check both crash conditions
    has_signal_11 = check_crash_signal()
    has_negative_seqno = check_grastate_seqno()
    
    print(f"  Signal 11 in log: {'Yes' if has_signal_11 else 'No'}")
    print(f"  Seqno is -1: {'Yes' if has_negative_seqno else 'No'}")
    
    # Both conditions must be true to proceed
    if not (has_signal_11 and has_negative_seqno):
        print("\nCrash conditions not met. Exiting.")
        sys.exit(0)
    
    # Alert user about crash
    print("\n" + "!" * 50)
    print("ALERT: mysqld has crashed!")
    print("!" * 50)
    
    # Check sudo permissions
    print("\nChecking sudo permissions...")
    if not check_sudo_permissions():
        print("\nERROR: Sudo permissions are required to recover mysqld.")
        print("Please run this script with a user that has sudo access.")
        sys.exit(1)
    
    print("✓ Sudo permissions confirmed")
    
    # Attempt recovery
    print("\nAttempting crash recovery...")
    if not run_wsrep_recover():
        print("ERROR: Failed to run wsrep-recover")
        sys.exit(1)
    
    # Wait a moment for logs to be written
    time.sleep(2)
    
    # Parse wsrep position from log
    print("\nParsing wsrep position from log...")
    gid_string, seqno_string = parse_wsrep_position()
    
    if gid_string is None or seqno_string is None:
        print("ERROR: Could not find wsrep position in log")
        sys.exit(1)
    
    print(f"Found wsrep position - GID: {gid_string}, Seqno: {seqno_string}")
    
    # Update grastate.dat with new seqno
    print(f"\nUpdating grastate.dat with new seqno: {seqno_string}")
    if update_grastate_seqno(new_seqno=seqno_string):
        print("\n✓ Recovery process completed successfully!")
        print("You can now attempt to restart the mysqld service.")
    else:
        print("\n✗ Failed to update grastate.dat")
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nScript interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        sys.exit(1)
