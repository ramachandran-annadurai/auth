#!/usr/bin/env python3
"""
Generate comprehensive test report for Patient Auth Service
"""
import os
import sys
import subprocess
import json
from datetime import datetime

def run_command(command, capture_output=True):
    """Run command and return result"""
    try:
        result = subprocess.run(
            command, 
            shell=True, 
            capture_output=capture_output, 
            text=True,
            cwd=os.path.dirname(os.path.abspath(__file__))
        )
        return result.returncode == 0, result.stdout, result.stderr
    except Exception as e:
        return False, "", str(e)

def generate_test_report():
    """Generate comprehensive test report"""
    print("🧪 Generating Comprehensive Test Report for Patient Auth Service")
    print("=" * 80)
    
    report = {
        "timestamp": datetime.now().isoformat(),
        "service": "Patient & Doctor Auth Service",
        "version": "1.0.0",
        "test_results": {},
        "coverage": {},
        "summary": {}
    }
    
    # Test categories to run
    test_categories = {
        "security": "tests/test_security.py",
        "config": "tests/test_config.py", 
        "email_service": "tests/test_email_service.py",
        "auth_routes": "tests/test_auth_routes.py",
        "auth_service": "tests/test_auth_service.py",
        "integration": "tests/test_integration.py"
    }
    
    total_passed = 0
    total_failed = 0
    total_tests = 0
    
    # Run each test category
    for category, test_file in test_categories.items():
        print(f"\n📋 Running {category.upper()} Tests...")
        print("-" * 50)
        
        # Run pytest for this category
        success, stdout, stderr = run_command(f"python -m pytest {test_file} -v --tb=short")
        
        # Parse results
        if success:
            lines = stdout.split('\n')
            passed_count = 0
            failed_count = 0
            test_details = []
            
            for line in lines:
                if "PASSED" in line:
                    passed_count += 1
                    test_name = line.split("::")[1].split(" ")[0] if "::" in line else "unknown"
                    test_details.append({"name": test_name, "status": "PASSED"})
                elif "FAILED" in line:
                    failed_count += 1
                    test_name = line.split("::")[1].split(" ")[0] if "::" in line else "unknown"
                    test_details.append({"name": test_name, "status": "FAILED"})
            
            report["test_results"][category] = {
                "status": "PASSED" if failed_count == 0 else "FAILED",
                "passed": passed_count,
                "failed": failed_count,
                "total": passed_count + failed_count,
                "details": test_details,
                "output": stdout[-1000:] if len(stdout) > 1000 else stdout  # Last 1000 chars
            }
            
            total_passed += passed_count
            total_failed += failed_count
            total_tests += passed_count + failed_count
            
            print(f"✅ {category}: {passed_count} passed, {failed_count} failed")
            
        else:
            report["test_results"][category] = {
                "status": "ERROR",
                "passed": 0,
                "failed": 0,
                "total": 0,
                "error": stderr,
                "output": stdout
            }
            print(f"❌ {category}: Error running tests")
            if stderr:
                print(f"Error: {stderr[:200]}...")
    
    # Generate overall summary
    report["summary"] = {
        "total_tests": total_tests,
        "total_passed": total_passed,
        "total_failed": total_failed,
        "success_rate": round((total_passed / total_tests * 100) if total_tests > 0 else 0, 2),
        "status": "PASSED" if total_failed == 0 else "FAILED"
    }
    
    # Try to run coverage
    print(f"\n📊 Running Coverage Analysis...")
    print("-" * 50)
    
    coverage_success, coverage_stdout, coverage_stderr = run_command(
        "python -m pytest tests/ --cov=app --cov-report=term --cov-report=html"
    )
    
    if coverage_success:
        # Parse coverage from output
        coverage_lines = coverage_stdout.split('\n')
        for line in coverage_lines:
            if "TOTAL" in line and "%" in line:
                parts = line.split()
                if len(parts) >= 4:
                    try:
                        coverage_percent = parts[-1].replace('%', '')
                        report["coverage"]["total_percentage"] = float(coverage_percent)
                        break
                    except:
                        pass
        
        report["coverage"]["status"] = "SUCCESS"
        report["coverage"]["output"] = coverage_stdout[-500:] if len(coverage_stdout) > 500 else coverage_stdout
        print("✅ Coverage analysis completed")
    else:
        report["coverage"]["status"] = "ERROR"
        report["coverage"]["error"] = coverage_stderr
        print("❌ Coverage analysis failed")
    
    return report

def print_summary_report(report):
    """Print formatted summary report"""
    print("\n" + "=" * 80)
    print("🎯 TEST EXECUTION SUMMARY REPORT")
    print("=" * 80)
    
    print(f"📅 Generated: {report['timestamp']}")
    print(f"🏥 Service: {report['service']}")
    print(f"📦 Version: {report['version']}")
    
    print(f"\n📊 OVERALL RESULTS:")
    summary = report['summary']
    print(f"   Total Tests: {summary['total_tests']}")
    print(f"   ✅ Passed: {summary['total_passed']}")
    print(f"   ❌ Failed: {summary['total_failed']}")
    print(f"   📈 Success Rate: {summary['success_rate']}%")
    print(f"   🎯 Overall Status: {summary['status']}")
    
    if 'total_percentage' in report.get('coverage', {}):
        print(f"   📋 Code Coverage: {report['coverage']['total_percentage']}%")
    
    print(f"\n📋 DETAILED RESULTS BY CATEGORY:")
    for category, results in report['test_results'].items():
        status_icon = "✅" if results['status'] == "PASSED" else "❌" if results['status'] == "FAILED" else "⚠️"
        print(f"   {status_icon} {category.upper()}: {results['passed']} passed, {results['failed']} failed")
        
        # Show failed test details
        if results['failed'] > 0 and 'details' in results:
            failed_tests = [test for test in results['details'] if test['status'] == 'FAILED']
            for test in failed_tests[:3]:  # Show first 3 failed tests
                print(f"      💥 {test['name']}")
    
    print(f"\n📁 REPORTS GENERATED:")
    print(f"   📄 test_report.json - Detailed JSON report")
    print(f"   📊 htmlcov/index.html - Coverage report (if successful)")
    print(f"   📋 TEST_RESULTS.md - Markdown summary")

def save_markdown_report(report):
    """Save report as markdown file"""
    md_content = f"""# 🧪 Test Execution Report - Patient Auth Service

**Generated:** {report['timestamp']}  
**Service:** {report['service']}  
**Version:** {report['version']}

## 📊 Overall Summary

| Metric | Value |
|--------|-------|
| Total Tests | {report['summary']['total_tests']} |
| ✅ Passed | {report['summary']['total_passed']} |
| ❌ Failed | {report['summary']['total_failed']} |
| Success Rate | {report['summary']['success_rate']}% |
| Overall Status | **{report['summary']['status']}** |
"""

    if 'total_percentage' in report.get('coverage', {}):
        md_content += f"| Code Coverage | {report['coverage']['total_percentage']}% |\n"

    md_content += "\n## 📋 Test Results by Category\n\n"
    
    for category, results in report['test_results'].items():
        status_emoji = "✅" if results['status'] == "PASSED" else "❌" if results['status'] == "FAILED" else "⚠️"
        md_content += f"### {status_emoji} {category.upper()}\n\n"
        md_content += f"- **Status:** {results['status']}\n"
        md_content += f"- **Passed:** {results['passed']}\n"
        md_content += f"- **Failed:** {results['failed']}\n"
        md_content += f"- **Total:** {results['total']}\n\n"
        
        if 'details' in results and results['details']:
            md_content += "**Test Details:**\n"
            for test in results['details']:
                status_icon = "✅" if test['status'] == "PASSED" else "❌"
                md_content += f"- {status_icon} `{test['name']}`\n"
            md_content += "\n"
    
    if report.get('coverage', {}).get('status') == 'SUCCESS':
        md_content += "## 📊 Coverage Report\n\n"
        md_content += f"- **Overall Coverage:** {report['coverage'].get('total_percentage', 'N/A')}%\n"
        md_content += "- **Detailed Report:** `htmlcov/index.html`\n\n"
    
    md_content += """## 🚀 Next Steps

1. **Fix Failed Tests:** Address any failing test cases
2. **Improve Coverage:** Target areas with low test coverage
3. **Review Reports:** Check detailed HTML coverage report
4. **Documentation:** Update test documentation if needed

## 📁 Generated Files

- `test_report.json` - Complete test results in JSON format
- `htmlcov/index.html` - Interactive coverage report
- `TEST_RESULTS.md` - This summary report
"""
    
    with open('TEST_RESULTS.md', 'w', encoding='utf-8') as f:
        f.write(md_content)

def main():
    """Main execution"""
    print("Starting comprehensive test execution...")
    
    # Generate test report
    report = generate_test_report()
    
    # Save JSON report
    with open('test_report.json', 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    # Save markdown report
    save_markdown_report(report)
    
    # Print summary
    print_summary_report(report)
    
    print(f"\n🎉 Test execution completed!")
    print(f"📁 Reports saved:")
    print(f"   - test_report.json")
    print(f"   - TEST_RESULTS.md")
    if report.get('coverage', {}).get('status') == 'SUCCESS':
        print(f"   - htmlcov/index.html")

if __name__ == "__main__":
    main()
