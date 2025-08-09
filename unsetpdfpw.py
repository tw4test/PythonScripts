import pikepdf

def remove_pdf_password(input_pdf, output_pdf, password):
    try:
        # Open the password-protected PDF with the provided password
        pdf = pikepdf.open(input_pdf, password=password)
        
        # Save the PDF without encryption (removes the password)
        pdf.save(output_pdf)
        
        print(f"Password removed successfully! Saved as {output_pdf}")
        
    except pikepdf.PasswordError:
        print("Incorrect password provided.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

# Example usage
if __name__ == "__main__":
    # Specify your input file, output file, and password
    input_file = "c:/temp/input.pdf"  # Replace with your input PDF file path
    output_file = "c:/temp/output.pdf"  # Replace with your desired output file path
    pdf_password = "11111111"  # Replace with the actual password
    
    # Call the function to remove the password
    remove_pdf_password(input_file, output_file, pdf_password)