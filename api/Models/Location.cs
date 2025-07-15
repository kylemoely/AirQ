using System;
using System.Threading.Tasks;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Mvc.ModelBinding.Validation;

namespace api.Models
{
	public class Location
	{
		public int Id { get; set } // PRIMARY KEY
		public string Name { get; set; }
		public double Latitude { get; set; }
		public double Longitude { get; set; }
		public int Country_id { get; set; } // FOREIGN KEY REFERENCES Countries

		// [JsonIgnore]
		// public Country? AssignedCountry { get; set; }

		public Location(int id, string name, double latitude, double longitude, int country_id)
		{
			Id = id;
			Name = name;
			Latitude = latitude;
			Longitude = longitude;
			Country_id = country_id;
		}
	}
}
