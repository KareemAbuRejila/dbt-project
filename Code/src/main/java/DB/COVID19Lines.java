package DB;
import java.util.Date;

public class COVID19Lines {
	//ID,age,sex,city,province,country, latitude,longitude,date_admission_hospital,date_confirmation,lives_in_Wuhan
	private  String ID;
	private  String age;
	private  String sex ;
	private  String city;
	private  String province;
	private  String country;
	private  String latitude;
	private  String longitude;
	private  String date_admission_hospital;
	private  String date_confirmation;
	private  String lives_in_Wuhan;

	public COVID19Lines(String ID, String age, String sex, String city, String province, String country, String latitude, String longitude, String date_admission_hospital, String date_confirmation, String lives_in_Wuhan) {
		this.ID = ID;
		this.age = age;
		this.sex = sex;
		this.city = city;
		this.province = province;
		this.country = country;
		this.latitude = latitude;
		this.longitude = longitude;
		this.date_admission_hospital = date_admission_hospital;
		this.date_confirmation = date_confirmation;
		this.lives_in_Wuhan = lives_in_Wuhan;
	}

	public String getID() {
		return ID;
	}

	public void setID(String ID) {
		this.ID = ID;
	}

	public String getAge() {
		return age;
	}

	public void setAge(String age) {
		this.age = age;
	}

	public String getSex() {
		return sex;
	}

	public void setSex(String sex) {
		this.sex = sex;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getProvince() {
		return province;
	}

	public void setProvince(String province) {
		this.province = province;
	}

	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}

	public String getLatitude() {
		return latitude;
	}

	public void setLatitude(String latitude) {
		this.latitude = latitude;
	}

	public String getLongitude() {
		return longitude;
	}

	public void setLongitude(String longitude) {
		this.longitude = longitude;
	}

	public String getDate_admission_hospital() {
		return date_admission_hospital;
	}

	public void setDate_admission_hospital(String date_admission_hospital) {
		this.date_admission_hospital = date_admission_hospital;
	}

	public String getDate_confirmation() {
		return date_confirmation;
	}

	public void setDate_confirmation(String date_confirmation) {
		this.date_confirmation = date_confirmation;
	}

	public String getLives_in_Wuhan() {
		return lives_in_Wuhan;
	}

	public void setLives_in_Wuhan(String lives_in_Wuhan) {
		this.lives_in_Wuhan = lives_in_Wuhan;
	}
}
