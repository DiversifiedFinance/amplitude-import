cd data
gunzip -c *.gz > output.json
sed -E 's/\$insert_id/insert_id/g' output.json > output_fixed.json
cd ..
bundle exec ruby sample_import.rb ./data/output_fixed.json
