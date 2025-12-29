# cleanup_output.py
import os
import re

def clean_markdown_file(file_path):
    """Clean up markdown file formatting issues."""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original = content
    
    # 1. Remove any ```markdown or code fences at start/end
    content = content.strip()
    if content.startswith('```markdown'):
        content = content[11:].strip()
    elif content.startswith('```'):
        content = content[3:].strip()
    
    if content.endswith('```'):
        content = content[:-3].strip()
    
    # 2. Fix code block language identifiers
    # Replace ```bash with ```
    content = re.sub(r'```\s*(bash|csharp|markdown|python|javascript|json|html|css|sql|xml|yaml|ini)\n', '```\n', content)
    
    # 3. Remove emojis and icons
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F680-\U0001F6FF"  # transport & map symbols
        "\U0001F1E0-\U0001F1FF"  # flags (iOS)
        "\U00002500-\U00002BEF"  # chinese char
        "\U00002702-\U000027B0"
        "\U00002702-\U000027B0"
        "\U000024C2-\U0001F251"
        "\U0001f926-\U0001f937"
        "\U00010000-\U0010ffff"
        "\u2640-\u2642" 
        "\u2600-\u2B55"
        "\u200d"
        "\u23cf"
        "\u23e9"
        "\u231a"
        "\ufe0f"  # dingbats
        "\u3030"
        "]+", flags=re.UNICODE)
    content = emoji_pattern.sub('', content)
    
    # 4. Ensure YAML front matter starts correctly
    if not content.startswith('---'):
        content = '---\n' + content
    
    # 5. Remove any added images that weren't in original
    # Keep only images that match the original pattern
    if '![Conversion workflow diagram]' in content and '![Conversion workflow diagram]' not in original:
        # Remove this specific added image
        lines = content.split('\n')
        lines = [line for line in lines if '![Conversion workflow diagram]' not in line]
        content = '\n'.join(lines)
    
    # 6. Ensure proper code block formatting
    content = re.sub(r'```\s*\n\s*```', '', content)  # Remove empty code blocks
    
    if content != original:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"✅ Cleaned: {file_path}")
        return True
    
    return False

def clean_all_outputs():
    """Clean all optimized files."""
    output_dir = "optimized-posts"
    
    if not os.path.exists(output_dir):
        print(f"❌ Directory not found: {output_dir}")
        return
    
    cleaned_count = 0
    for root, dirs, files in os.walk(output_dir):
        for file in files:
            if file.endswith('.md'):
                file_path = os.path.join(root, file)
                if clean_markdown_file(file_path):
                    cleaned_count += 1
    
    print(f"\n📊 Cleaned {cleaned_count} files")

if __name__ == "__main__":
    clean_all_outputs()