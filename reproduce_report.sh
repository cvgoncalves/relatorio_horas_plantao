#!/bin/sh

# TODO Adicionar observacao SOBREAVISO pra qdo o numerod e horas for 0h
# TODO ADicionar filtro de dados, se tiver zero horas e zero reais, tirar do dataset e discriminado

start_time=$(date +'%Y-%m-%d %H:%M:%S')
SECONDS=0
echo "${start_time} Starting script..."

cd "$(dirname "$0")"

# Create the 'reports' directory if it doesn't exist
# mkdir -p reports_monthly

# Create a directory with the current month as its name
current_month=$(date +'%Y-%m')
report_dir="reports_monthly/${current_month}"
mkdir -p "${report_dir}"

data=$(date +'%d-%m-%Y')
echo="Reference date: ${data} ..."

# params=("171069" "150435" "162863" "177963" "194281" "187117")

# for n in "${params[@]}";
# do
# quarto render report.qmd --output "${n}.pdf" -P crm:"$n" -P date:"$data"
# done

# Read the parameters from the params.txt file
while IFS=',' read -r name crm
do
  echo "Starting report for CRM: ${crm}"
  if quarto render report.qmd --output-dir "${report_dir}" --output "${name}_${crm}_${current_month}.pdf" -P crm:"$crm" -P date:"$data"
  then
    echo "Report generated successfully for CRM: ${crm}"
  else
    echo "Failed to generate report for CRM: ${crm}"
    echo "$crm" >> "$failed_crms"
  fi
done < "params.txt"

end_time=$(date +'%Y-%m-%d %H:%M:%S')
echo "${end_time} Script finished."
elapsed_time=$SECONDS
echo "Elapsed time: ${elapsed_time} seconds"
